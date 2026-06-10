import os
import io
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Ép console dùng UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def main():
    print("[INFO] Bắt đầu mô hình Chấm điểm độ khó công việc (Complexity Scoring)...")
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(current_dir, "..", "data", "processed", "Data_ITJOB_Cleaned.csv")
    results_dir = os.path.join(current_dir, "..", "data", "mining_results")
    os.makedirs(results_dir, exist_ok=True)
    report_path = os.path.join(results_dir, "complexity_scoring_report.txt")
    plot_path = os.path.join(results_dir, "complexity_clusters.png")
    
    if not os.path.exists(data_path):
        print(f"[ERROR] Không tìm thấy file: {data_path}")
        return

    df = pd.read_csv(data_path)
    df['description_clean'] = df['description_clean'].fillna('')
    df['yoe_extracted'] = pd.to_numeric(df['yoe_extracted'], errors='coerce').fillna(0)
    df['skill_count'] = pd.to_numeric(df['skill_count'], errors='coerce').fillna(0)
    
    print("[INFO] Đo lường mật độ từ vựng chuyên ngành bằng TF-IDF...")
    tfidf = TfidfVectorizer(max_features=3000, stop_words='english')
    tfidf_matrix = tfidf.fit_transform(df['description_clean'])
    
    # Độ đậm đặc từ vựng (Vocabulary richness): Tính tổng điểm TF-IDF của mỗi Job
    # Job nào càng dùng nhiều từ khóa hiếm/chuyên ngành thì điểm này càng cao
    vocab_richness = np.array(tfidf_matrix.sum(axis=1)).flatten()
    
    # Kết hợp 3 đặc trưng để đánh giá độ khó
    features = pd.DataFrame({
        'Vocab_Richness': vocab_richness,
        'Skill_Count': df['skill_count'],
        'YOE_Required': df['yoe_extracted']
    })
    
    # Chuẩn hóa dữ liệu trước khi gom cụm
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(features)
    
    print("[INFO] Áp dụng K-Means để chia 3 cụm (Dễ, Trung bình, Khó)...")
    kmeans = KMeans(n_clusters=3, random_state=42)
    clusters = kmeans.fit_predict(scaled_features)
    
    features['Cluster'] = clusters
    
    # Gán nhãn Dễ/Trung bình/Khó tự động dựa trên giá trị trung bình của các cụm
    cluster_means = features.groupby('Cluster').mean()
    # Tính tổng giá trị (đã chuẩn hóa hoặc thô) để xếp hạng độ khó
    sorted_clusters = cluster_means.sum(axis=1).sort_values().index.tolist()
    
    mapping = {
        sorted_clusters[0]: '1 - Basic (Dễ)',
        sorted_clusters[1]: '2 - Intermediate (Trung Bình)',
        sorted_clusters[2]: '3 - Advanced (Khó nhằn)'
    }
    
    features['Complexity_Level'] = features['Cluster'].map(mapping)
    df['Complexity_Level'] = features['Complexity_Level']
    
    print("[INFO] Đang lưu báo cáo...")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=== BÁO CÁO ĐỘ PHỨC TẠP CÔNG VIỆC (COMPLEXITY SCORING) ===\n")
        f.write("(Đánh giá dựa trên Mật độ từ vựng chuyên môn TF-IDF, Số lượng kỹ năng và Năm kinh nghiệm)\n\n")
        
        counts = features['Complexity_Level'].value_counts().sort_index()
        for level, count in counts.items():
            f.write(f"--- MỨC ĐỘ: {level} ---\n")
            f.write(f"Số lượng tin tuyển dụng: {count} jobs\n")
            subset = features[features['Complexity_Level'] == level]
            f.write(f"-> Trung bình Năm kinh nghiệm (YOE) : {subset['YOE_Required'].mean():.2f} năm\n")
            f.write(f"-> Trung bình Số lượng kỹ năng      : {subset['Skill_Count'].mean():.2f} skills\n")
            f.write(f"-> Độ đậm đặc từ vựng chuyên môn    : {subset['Vocab_Richness'].mean():.2f} điểm TF-IDF\n\n")
            
    plt.figure(figsize=(10, 6))
    sns.scatterplot(
        data=features, 
        x='Skill_Count', 
        y='Vocab_Richness', 
        hue='Complexity_Level', 
        palette=['#2ecc71', '#f1c40f', '#e74c3c'],
        alpha=0.7,
        hue_order=['1 - Basic (Dễ)', '2 - Intermediate (Trung Bình)', '3 - Advanced (Khó nhằn)']
    )
    plt.title("Phân bổ Độ phức tạp Công việc (Job Complexity Clusters)", fontsize=14, fontweight='bold')
    plt.xlabel("Yêu cầu Số lượng Kỹ năng (Skill Count)")
    plt.ylabel("Độ đậm đặc Từ vựng chuyên ngành (TF-IDF Richness)")
    plt.tight_layout()
    plt.savefig(plot_path, dpi=300)
    plt.close()
    
    print(f"[OK] Đã lưu báo cáo: {report_path}")
    print(f"[OK] Đã lưu biểu đồ: {plot_path}")

if __name__ == "__main__":
    main()
