# Data Mining (ML) - IT Job Market

Thư mục này chứa các script thực hiện các kỹ thuật Khai phá Dữ liệu (Data Mining) và Học Máy (Machine Learning) trên tập dữ liệu tuyển dụng IT (`Data_ITJOB_Cleaned.csv`). 

Kết quả (report text và các biểu đồ) được lưu tự động vào thư mục `data/mining_results/`.

---

## 1. Luật Kết Hợp Kỹ Năng (Association Rules)
**File:** `dm_association_rules.py`
- Khám phá sự liên hệ giữa các kỹ năng IT thường được yêu cầu cùng nhau bằng thuật toán Apriori.
- **Đánh giá & Nhận xét:** Với min_support = 0.05, mô hình không tìm thấy luật nào quá mạnh (Lift > 1). Điều này phản ánh thực tế thị trường IT hiện đại: các kỹ năng công nghệ rất phân tán, linh hoạt và ít có những "combo" cứng nhắc như trước đây.

## 2. Phân Cụm Công Việc (Job Clustering)
**File:** `dm_job_clustering.py`
- Phân nhóm các bài đăng tuyển dụng thành các cụm có chung đặc tính về lương, số năm kinh nghiệm, loại hình làm việc bằng K-Means.
- **Đánh giá & Nhận xét:** Tìm được K=4 cụm tối ưu (Silhouette=0.4257). Đáng chú ý là cụm lương cao nhất trung bình ~51 triệu (5.3 năm kinh nghiệm). Có một cụm đặc trưng là việc làm Remote (Lương 36 triệu, 3.8 YOE), chứng minh làm Remote yêu cầu kinh nghiệm khá vững nhưng bù lại mức lương rất tốt.

## 3. Dự Đoán Mức Lương (Salary Prediction)
**File:** `dm_salary_prediction.py`
- Dự đoán mức lương dựa trên thông tin công việc, kỹ năng, và yêu cầu dùng Linear Regression, Random Forest, XGBoost.
- **Đánh giá & Nhận xét:** Thuật toán Linear Regression cho kết quả tốt nhất (R² = 0.6780, sai số MAE ~4.3 triệu VND). Tuy nhiên R² chưa chạm mốc 0.8+ cho thấy việc đoán chính xác lương IT là rất khó, vì nó phụ thuộc nhiều vào yếu tố không có trên giấy tờ (kỹ năng mềm, ngân sách cty, khả năng đàm phán).

## 4. Khai Phá Văn Bản (Text Mining)
**File:** `dm_text_mining.py`
- Khám phá và phân loại các chủ đề chính từ mô tả công việc bằng Topic Modeling (LDA) và WordCloud.
- **Đánh giá & Nhận xét:** Mô hình chia thành công 5 cụm chủ đề rõ nét. Các từ khóa tách biệt hẳn hoi: Cụm Công cụ (Power BI, SQL, Python), Cụm Quản trị/Nghiệp vụ (Business, Product, Management, Team), và các cụm từ khóa tiếng Việt về phúc lợi, yêu cầu.

## 5. Hệ Tư Vấn Công Việc (Job Recommendation)
**File:** `dm_job_recommendation.py`
- Gợi ý tin tuyển dụng phù hợp nhất dựa trên kỹ năng bằng Content-Based Filtering (TF-IDF + Cosine Similarity).
- **Đánh giá & Nhận xét:** Mô hình matching rất mượt giữa Keyword của người dùng nhập vào (VD: Data Engineer) và danh sách Title/Skills của tin tuyển dụng. Tuy đơn giản hơn bản Deep Learning trên web, nhưng chạy cực nhanh và hiệu quả tốt.

## 6. Phân Loại Cấp Bậc (Job Level Classification)
**File:** `dm_level_classification.py`
- Dự đoán cấp bậc (Fresher, Junior, Senior, Manager) dựa trên yêu cầu kinh nghiệm và mô tả bằng Random Forest.
- **Đánh giá & Nhận xét:** Đạt độ chính xác (Accuracy) cực kỳ ấn tượng 92%. Trong đó đoán cực chuẩn nhóm Manager (F1-score 0.98), Junior (0.90) và Senior (0.94). Điều này chứng minh từ ngữ trong phần mô tả của các JD Senior/Manager luôn có một "chuẩn mực" nhất định để AI nhận diện.

## 7. Phân Tích Mạng Lưới Kỹ Năng (Skill Network Analysis)
**File:** `dm_network_analysis.py`
- Xây dựng đồ thị liên kết kỹ năng, tìm Kỹ năng Lõi bằng PageRank và Degree Centrality.
- **Đánh giá & Nhận xét:** Mạng lưới trích xuất được 35 nodes và 56 liên kết mạnh (xuất hiện chung >= 15 lần). Các kỹ năng đứng đầu thuật toán PageRank là: **English, Java, Python, AI, SQL**. Đây chính xác là các "hạt nhân" kết nối đa ngành trong thị trường IT hiện nay.

## 8. Trích Xuất Thông Tin (Information Extraction)
**File:** `dm_information_extraction.py`
- Dùng NLP Rule-based và Regex để bóc tách phúc lợi lương tháng 13, Bảo hiểm, Remote, Kỹ năng mềm.
- **Đánh giá & Nhận xét:** Regex quét rất tốt văn bản tiếng Việt/Anh lẫn lộn, thống kê được bức tranh toàn cảnh về chế độ phúc lợi mà các công ty hứa hẹn trên JD.

---

## CÁC MÔ HÌNH MỞ RỘNG (DỰA TRÊN ĐỘC QUYỀN TF-IDF)

## 9. Phân Loại Mảng Chuyên Môn (Role Classification)
**File:** `dm_role_classification.py`
- **Mục tiêu:** Phân loại tự động JD thuộc mảng Backend, Frontend, Data, DevOps, hay Mobile.
- **Đánh giá & Nhận xét:** Độ chính xác tổng thể 79%. Phân loại xuất sắc nhất ở mảng QA & Testing (F1=0.87) và Data & AI (F1=0.82). Lý do vì hai mảng này xài những từ vựng chuyên ngành (test case, automation, machine learning) rất riêng biệt, không bị trùng lấp như nhóm Web/Mobile.

## 10. Khắc Họa Chân Dung Cấp Bậc (Job Level Profiling)
**File:** `dm_level_profiling.py`
- **Mục tiêu:** Tìm ra các từ khóa "tố cáo" và định hình tính chất của từng cấp bậc công việc.
- **Đánh giá & Nhận xét:** Thuật toán TF-IDF làm nổi bật cực kỳ chính xác xu hướng ngành. Mây từ vựng của Fresher/Junior ngập tràn các từ cơ bản, trong khi Manager đặc kịt các từ vựng thượng tầng như: *management, project, business, product*.

## 11. Chấm Điểm Độ Khó Công Việc (Complexity Scoring)
**File:** `dm_complexity_scoring.py`
- **Mục tiêu:** Đánh giá mức độ khắt khe và độ khó của một tin tuyển dụng (Dễ / Trung Bình / Khó nhằn).
- **Đánh giá & Nhận xét:** Thuật toán K-Means chia thành công 3 cụm. Cụm Khó (Advanced) yêu cầu lượng Skills và số năm kinh nghiệm vượt trội, đồng thời điểm tổng TF-IDF cũng rất cao (chứng tỏ JD sử dụng ngôn từ rất chuyên môn và khắt khe).
