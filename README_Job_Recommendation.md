# HỆ TƯ VẤN VIỆC LÀM (Job Recommendation System) - AI & NLP

Hệ thống này ứng dụng Xử lý ngôn ngữ tự nhiên (NLP) và Trí tuệ nhân tạo (AI) để gợi ý việc làm IT phù hợp nhất dựa trên kỹ năng hoặc CV của người dùng. Hệ thống gồm 2 giai đoạn chính:

## 1. Giai đoạn 1: Chuẩn bị dữ liệu và sinh Embedding (File: `Job_Recommendation_Train.ipynb`)
Mặc dù tên file có chữ "Train", nhưng bản chất đây là bước **Feature Extraction (Trích xuất đặc trưng)**, không có quá trình huấn luyện (fine-tuning) lại model.

- **Dữ liệu đầu vào**: Lấy từ file `Data_ITJOB_Cleaned.csv`.
- **Gộp văn bản (Text Concatenation)**: Ghép 3 trường thông tin quan trọng nhất của mỗi công việc (`title_clean`, `skills_clean`, `description_clean`) thành một chuỗi duy nhất (`combined_text`).
- **Sinh Embedding**: 
  - Sử dụng Pre-trained model đa ngôn ngữ của Hugging Face: `paraphrase-multilingual-MiniLM-L12-v2`.
  - Model này hoạt động như một "dịch giả", đọc hàng ngàn chuỗi `combined_text` và chuyển hóa chúng thành các ma trận/vector số học (Embeddings) để máy tính hiểu được ý nghĩa ngữ nghĩa (Semantic meaning) của từng công việc.
- **Lưu trữ**: Xuất ra 2 file: `job_data.csv` (lưu text) và `job_embeddings.npy` (lưu ma trận số) để sẵn sàng cho bước suy luận. Việc này giúp tiết kiệm thời gian, không phải sinh lại vector mỗi lần chạy.

## 2. Giai đoạn 2: Suy luận và Gợi ý (File: `Job_Recommendation_Inference.ipynb`)
Đây là bước mô phỏng trải nghiệm thực tế của người dùng (End-User).

- **Input**: Người dùng nhập đoạn văn bản mô tả kỹ năng hoặc CV (Ví dụ: *"Python Data Engineer biết SQL, Machine Learning"*).
- **Vector hóa Input**: Dùng lại model `paraphrase-multilingual-MiniLM-L12-v2` để chuyển đoạn text của người dùng thành một vector số duy nhất.
- **Tính toán độ tương đồng (Matching)**: 
  - Sử dụng thuật toán toán học **Cosine Similarity**.
  - Thuật toán sẽ đo khoảng cách giữa vector của người dùng và hàng ngàn vector của công việc (đã lưu trong `job_embeddings.npy`). 
  - Hai vector có hướng càng giống nhau (điểm Cosine tiến về 1), nghĩa là kỹ năng của ứng viên càng khớp với yêu cầu của công ty.
- **Output**: Trả về Top K (VD: 5) công việc có điểm Cosine Similarity cao nhất, sắp xếp từ cao xuống thấp.

---

### Tóm tắt giá trị kỹ thuật
- Không dùng Keyword matching (so khớp từ khóa) cứng nhắc, mà dùng **Semantic matching (So khớp ngữ nghĩa)**. Dù ứng viên viết "Artificial Intelligence" nhưng JD ghi "AI", model NLP vẫn hiểu chúng là một.
- Dùng model `multilingual` hỗ trợ tốt cả tiếng Việt và tiếng Anh (phù hợp với thị trường IT Việt Nam hay trộn lẫn ngôn ngữ).
