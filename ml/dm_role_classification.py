import os
import io
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.svm import LinearSVC
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.feature_extraction.text import TfidfVectorizer

# Ép console dùng UTF-8 để không lỗi tiếng Việt trên Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def map_role(title):
    title = str(title).lower()
    if any(k in title for k in ['data', 'ai', 'machine learning', 'nlp', 'deep learning']):
        return 'Data & AI'
    elif any(k in title for k in ['backend', 'front end', 'frontend', 'web', 'full stack', 'fullstack', 'php', 'node', 'react']):
        return 'Web Development'
    elif any(k in title for k in ['mobile', 'ios', 'android', 'flutter', 'react native']):
        return 'Mobile Development'
    elif any(k in title for k in ['devops', 'sysadmin', 'system', 'cloud', 'aws', 'azure', 'infrastructure', 'security', 'network']):
        return 'System & DevOps'
    elif any(k in title for k in ['tester', 'qa', 'qc', 'test']):
        return 'QA & Testing'
    else:
        return 'Others'

def main():
    print("[INFO] Bắt đầu mô hình Phân loại mảng chuyên môn (Role Classification)...")
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(current_dir, "..", "data", "processed", "Data_ITJOB_Cleaned.csv")
    results_dir = os.path.join(current_dir, "..", "data", "mining_results")
    os.makedirs(results_dir, exist_ok=True)
    report_path = os.path.join(results_dir, "role_classification_report.txt")
    plot_path = os.path.join(results_dir, "role_confusion_matrix.png")
    
    if not os.path.exists(data_path):
        print(f"[ERROR] Không tìm thấy file: {data_path}")
        return

    df = pd.read_csv(data_path)
    df['title_clean'] = df['title_clean'].fillna('')
    df['description_clean'] = df['description_clean'].fillna('')
    
    # Phân loại tự động dựa trên Title
    df['Role'] = df['title_clean'].apply(map_role)
    
    # Lọc bỏ 'Others' để tập trung vào 5 mảng chính
    df = df[df['Role'] != 'Others']
    
    if len(df) < 50:
        print("[ERROR] Không đủ dữ liệu để train.")
        return
        
    X = df['description_clean']
    y = df['Role']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    print("[INFO] Vector hóa văn bản bằng TF-IDF...")
    tfidf = TfidfVectorizer(max_features=3000, stop_words='english')
    X_train_vec = tfidf.fit_transform(X_train)
    X_test_vec = tfidf.transform(X_test)
    
    print("[INFO] Đang huấn luyện thuật toán Linear SVC...")
    clf = LinearSVC(random_state=42, class_weight='balanced')
    clf.fit(X_train_vec, y_train)
    
    y_pred = clf.predict(X_test_vec)
    
    report = classification_report(y_test, y_pred, zero_division=0)
    cm = confusion_matrix(y_test, y_pred, labels=clf.classes_)
    
    print("[INFO] Đang lưu báo cáo...")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=== BÁO CÁO PHÂN LOẠI MẢNG CHUYÊN MÔN (ROLE CLASSIFICATION) ===\n\n")
        f.write(f"Số lượng mẫu huấn luyện: {X_train.shape[0]}\n")
        f.write(f"Số lượng mẫu kiểm thử: {X_test.shape[0]}\n\n")
        f.write("--- CLASSIFICATION REPORT ---\n")
        f.write(report)
        
    plt.figure(figsize=(10, 8))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Oranges', xticklabels=clf.classes_, yticklabels=clf.classes_)
    plt.title("Confusion Matrix - Role Classification")
    plt.ylabel('Thực tế (True Role)')
    plt.xlabel('Dự đoán (Predicted Role)')
    plt.tight_layout()
    plt.savefig(plot_path)
    plt.close()
    
    print(f"[OK] Đã lưu báo cáo: {report_path}")
    print(f"[OK] Đã lưu biểu đồ: {plot_path}")

if __name__ == "__main__":
    main()
