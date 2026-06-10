import os
import io
import sys
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.feature_extraction.text import TfidfVectorizer
from wordcloud import WordCloud

# Ép console dùng UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def main():
    print("[INFO] Bắt đầu mô hình Khắc họa chân dung cấp bậc (Job Level Profiling)...")
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(current_dir, "..", "data", "processed", "Data_ITJOB_Cleaned.csv")
    results_dir = os.path.join(current_dir, "..", "data", "mining_results")
    os.makedirs(results_dir, exist_ok=True)
    report_path = os.path.join(results_dir, "level_profiling_report.txt")
    plot_path = os.path.join(results_dir, "level_wordclouds.png")
    
    if not os.path.exists(data_path):
        print(f"[ERROR] Không tìm thấy file: {data_path}")
        return

    df = pd.read_csv(data_path)
    df['description_clean'] = df['description_clean'].fillna('')
    df['job_level'] = df['job_level'].fillna('Unknown')
    
    # Lọc ra 4 cấp bậc chính
    target_levels = ['Fresher', 'Junior', 'Senior', 'Manager']
    df = df[df['job_level'].isin(target_levels)]
    
    if len(df) == 0:
        print("[ERROR] Không đủ dữ liệu cho các cấp bậc.")
        return
        
    print("[INFO] Đang gom nhóm văn bản theo cấp bậc và chạy TF-IDF...")
    # Nối tất cả JD của cùng 1 cấp bậc thành 1 siêu văn bản (Document)
    grouped_text = df.groupby('job_level')['description_clean'].apply(lambda x: ' '.join(x)).reset_index()
    
    # Tính TF-IDF để tìm từ khóa "đặc trưng" nhất cho mỗi cấp bậc so với các cấp bậc khác
    tfidf = TfidfVectorizer(max_features=1500, stop_words='english', ngram_range=(1, 2))
    tfidf_matrix = tfidf.fit_transform(grouped_text['description_clean'])
    feature_names = tfidf.get_feature_names_out()
    
    dense = tfidf_matrix.todense()
    
    print("[INFO] Đang vẽ WordClouds...")
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    axes = axes.flatten()
    
    report_content = "=== BÁO CÁO TỪ KHÓA ĐẶC TRƯNG THEO CẤP BẬC (LEVEL PROFILING) ===\n"
    report_content += "(Dựa trên thuật toán TF-IDF, lọc ra những từ định hình sự khác biệt giữa các level)\n\n"
    
    for i, level in enumerate(grouped_text['job_level']):
        row = dense[i].tolist()[0]
        # Lấy điểm TF-IDF của từng từ khóa cho level này
        word_scores = [(feature_names[j], row[j]) for j in range(len(feature_names)) if row[j] > 0]
        word_scores = sorted(word_scores, key=lambda x: x[1], reverse=True)[:40]
        
        # Thêm vào báo cáo TXT
        report_content += f"--- {level.upper()} ---\n"
        for w, s in word_scores[:15]:
            report_content += f"- {w:<20}: {s:.4f}\n"
        report_content += "\n"
        
        # Vẽ WordCloud
        freq_dict = {w: s for w, s in word_scores}
        wc = WordCloud(width=600, height=400, background_color='white', colormap='magma').generate_from_frequencies(freq_dict)
        axes[i].imshow(wc, interpolation='bilinear')
        axes[i].set_title(f"{level} Core Keywords", fontsize=18, fontweight='bold')
        axes[i].axis('off')
        
    plt.tight_layout()
    plt.savefig(plot_path, dpi=300)
    plt.close()
    
    print("[INFO] Đang lưu báo cáo...")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_content)
        
    print(f"[OK] Đã lưu báo cáo: {report_path}")
    print(f"[OK] Đã lưu biểu đồ: {plot_path}")

if __name__ == "__main__":
    main()
