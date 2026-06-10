import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.feature_extraction.text import TfidfVectorizer
from scipy.sparse import hstack

def main():
    print("[INFO] Bắt đầu mô hình Phân loại cấp bậc (Job Level Classification)...")
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(current_dir, "..", "data", "processed", "Data_ITJOB_Cleaned.csv")
    results_dir = os.path.join(current_dir, "..", "data", "mining_results")
    os.makedirs(results_dir, exist_ok=True)
    report_path = os.path.join(results_dir, "level_classification_report.txt")
    plot_path = os.path.join(results_dir, "level_confusion_matrix.png")
    
    if not os.path.exists(data_path):
        print(f"[ERROR] Không tìm thấy file: {data_path}")
        return

    df = pd.read_csv(data_path)
    
    # Filter valid levels (ignore Undefined or missing)
    df = df[df['job_level'].notna() & (df['job_level'] != 'Undefined')]
    
    # Fill NAs
    df['yoe_extracted'] = df['yoe_extracted'].fillna(0)
    df['skill_count'] = df['skill_count'].fillna(0)
    df['skills_clean'] = df['skills_clean'].fillna('')
    df['title_clean'] = df['title_clean'].fillna('')
    
    print(f"[INFO] Dữ liệu hợp lệ để train: {len(df)} dòng.")
    if len(df) < 50:
        print("[ERROR] Không đủ dữ liệu để train.")
        return
        
    # Text features
    tfidf = TfidfVectorizer(max_features=1000, stop_words='english')
    text_features = tfidf.fit_transform(df['title_clean'] + " " + df['skills_clean'])
    
    # Numeric features
    num_features = df[['yoe_extracted', 'skill_count']].values
    
    # Combine features
    X = hstack([num_features, text_features])
    y = df['job_level']
    
    # Split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Train
    print("[INFO] Đang huấn luyện mô hình Random Forest...")
    clf = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
    clf.fit(X_train, y_train)
    
    # Predict
    y_pred = clf.predict(X_test)
    
    # Evaluation
    report = classification_report(y_test, y_pred, zero_division=0)
    cm = confusion_matrix(y_test, y_pred, labels=clf.classes_)
    
    print("[INFO] Đang lưu báo cáo và biểu đồ...")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=== BÁO CÁO PHÂN LOẠI CẤP BẬC CÔNG VIỆC (JOB LEVEL) ===\n\n")
        f.write(f"Thuật toán: Random Forest Classifier\n")
        f.write(f"Số lượng mẫu huấn luyện: {X_train.shape[0]}\n")
        f.write(f"Số lượng mẫu kiểm thử: {X_test.shape[0]}\n")
        f.write("\n--- CLASSIFICATION REPORT ---\n")
        f.write(report)
        f.write("\n")
        
    # Plot CM
    plt.figure(figsize=(10, 8))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', xticklabels=clf.classes_, yticklabels=clf.classes_)
    plt.title("Confusion Matrix - Job Level Classification")
    plt.ylabel('Thực tế (True Label)')
    plt.xlabel('Dự đoán (Predicted Label)')
    plt.tight_layout()
    plt.savefig(plot_path)
    plt.close()
    
    print(f"[OK] Đã lưu báo cáo: {report_path}")
    print(f"[OK] Đã lưu biểu đồ: {plot_path}")

if __name__ == "__main__":
    main()
