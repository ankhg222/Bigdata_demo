# 🗂️ Cụm MapReduce (Apache Spark) — Phân tích Thị Trường Việc Làm IT Việt Nam

Thư mục này chứa toàn bộ các kịch bản **PySpark MapReduce** dùng để xử lý dữ liệu lớn (Big Data) về tuyển dụng IT. Hệ thống thực hiện bóc tách, ánh xạ (Map), và tổng hợp (Reduce) để rút trích các insight sâu từ hàng ngàn tin tuyển dụng.

Dữ liệu đầu vào chung: `D:/HDFS/JOB_MARKET_BIGDATA/data/processed/Data_ITJOB_Cleaned.csv`
Dữ liệu đầu ra: Ghi song song định dạng Parquet (để load DB) và TXT (để đọc báo cáo) tại `data/parquet/`.

---

## 📋 Danh sách 15 tiến trình MapReduce

| # | File | Tên Job | Kỹ thuật & Mục tiêu Phân tích |
|---|------|---------|-------------------------------|
| 1 | `mr_location.py` | Location Analysis | Phân bố địa lý của việc làm (Map & đếm tần suất cơ bản) |
| 2 | `mr_top_skills.py` | Top Skills Extraction | Bóc tách mảng kỹ năng ẩn (Explode) để đếm Top 20 công nghệ |
| 3 | `mr_salary_by_level.py` | Salary vs Level | Gom nhóm (GroupBy) để tính Min/Max/Avg/Median lương theo cấp bậc |
| 4 | `mr_salary_by_skill.py` | Salary vs Skill | Bóc tách đa chiều để tìm Top 15 kỹ năng mang lại thu nhập cao nhất |
| 5 | `mr_yoe_salary_correlation.py` | YOE Correlation | Chia xô (Bucketing) kinh nghiệm và dùng Window Function tính mức tăng lương |
| 6 | `mr_company_hiring.py` | Company Hiring | Tìm Top 20 "ông lớn" tuyển dụng bằng cách tính Aggregate nhiều cột |
| 7 | `mr_remote_analysis.py` | Remote Trend | Phân tích chéo (Cross-Tabulation) xu hướng Remote/On-site với Mức lương |
| 8 | `mr_job_level_distribution.py` | Platform Distribution | Dùng Pivot để xoay trục dữ liệu: Phân bố cấp bậc theo từng Nguồn tuyển dụng |
| 9 | `mr_skill_cooccurrence.py` | Market Basket | Khai phá Bộ đôi Kỹ năng (Skill Co-occurrence) |
| 10 | `mr_top_paying_jobs.py` | Top-N Window | Phân vạch Window tìm Top 3 việc lương cao nhất từng cấp bậc |
| 11 | `mr_cross_tabulation.py` | Pivot Cross-Tab | Phân tích đa chiều Lương x Kỹ năng x Địa điểm bằng Pivot |
| 12 | `mr_salary_bracketing.py` | Salary Histogram | Phân khúc tiền lương (Bucketing) tự động bằng MapReduce |
| 13 | `mr_skill_rarity.py` | TF-IDF from scratch | Tính độ hiếm của kỹ năng bằng công thức IDF nguyên thủy |
| 14 | `mr_salary_outliers.py` | Anomaly Detection | Tìm bất thường: Các tin tuyển dụng có lương cao vượt ngưỡng IQR |
| 15 | `mr_olap_cube.py` | OLAP Data Cube | Cuộn khối đa chiều: Tính Sub-total/Grand-total cho Địa Điểm x Cấp Bậc x Remote |

---

## 🔍 Chi Tiết Kỹ Thuật (Architecture)

### 📄 1. `mr_location.py` — Location Analysis
**Mục tiêu:** Thống kê mật độ việc làm IT theo các trung tâm kinh tế (HCM, Hà Nội, Đà Nẵng).
**Quy trình MapReduce:**
- **MAP:** Lấy cột `location_clean`.
- **REDUCE:** `groupBy(location_clean).count()` → Đếm tổng và sort giảm dần.
- **Output:** `data/parquet/location_result/`

### 📄 2. `mr_top_skills.py` — Top Skills Analysis
**Mục tiêu:** Xác định top 20 kỹ năng IT đang khát nhân lực nhất.
**Quy trình MapReduce:**
- **MAP:** Hàm `explode()` bẻ chuỗi `skills_clean` thành n bản ghi riêng biệt `(skill, 1)`.
- **REDUCE:** `groupBy(skill).agg(count)` → Tổng hợp và sắp xếp để lấy Top 20.
- **Output:** `data/parquet/top_skills/`

### 📄 3. `mr_salary_by_level.py` — Salary by Job Level
**Mục tiêu:** Thống kê phổ lương (min/avg/median/max) cho từng cấp bậc (Fresher, Junior, Senior...).
**Quy trình MapReduce:**
- **MAP:** Trích xuất `(job_level, salary, yoe)`.
- **REDUCE:** `groupBy(job_level)` và tính tập hợp các tham số thống kê (min, max, avg, percentile_approx cho median).
- **WINDOW:** Bổ sung hàm Window để xếp hạng (`rank()`) các cấp bậc theo mức lương trung bình.
- **Output:** `data/parquet/salary_by_level/`

### 📄 4. `mr_salary_by_skill.py` — Salary by Skill
**Mục tiêu:** Trả lời câu hỏi "Học kỹ năng nào lương cao nhất?".
**Quy trình MapReduce:**
- **MAP:** Tương tự Job 2 nhưng map cặp giá trị `(skill, salary)`.
- **REDUCE:** Group theo `skill`, tính trung bình lương.
- **FILTER:** Loại bỏ nhiễu bằng cách chỉ tính những kỹ năng xuất hiện >= 10 lần trên thị trường.
- **Output:** `data/parquet/salary_by_skill/`

### 📄 5. `mr_yoe_salary_correlation.py` — YOE vs Salary Correlation
**Mục tiêu:** Phân tích tương quan: Thêm 1 năm kinh nghiệm thì lương tăng bao nhiêu?
**Quy trình MapReduce:**
- **MAP:** Viết logic chia xô (Bucketing) năm kinh nghiệm: `0-1yr`, `2yr`, `3-4yr`, `5-6yr`...
- **REDUCE:** `groupBy(yoe_bucket)` để tính lương trung bình từng xô.
- **WINDOW:** Dùng hàm `LAG()` của Spark Window để trừ mức lương của xô hiện tại với xô trước đó, ra được mức tăng lương kỳ vọng.
- **Output:** `data/parquet/yoe_salary_correlation/`

### 📄 6. `mr_company_hiring.py` — Company Hiring Analysis
**Mục tiêu:** Xếp hạng các công ty IT khát nhân lực nhất và phổ lương họ sẵn sàng trả.
**Quy trình MapReduce:**
- **MAP:** Map `(company, salary, job_level)`.
- **REDUCE:** Group theo công ty, đếm tổng job, tính lương TB, và gom mảng cấp bậc (`collect_set`).
- **BROADCAST:** Tính `% thị phần tuyển dụng` bằng cách Broadcast tổng số job toàn thị trường vào hàm chia.
- **Output:** `data/parquet/company_hiring/`

### 📄 7. `mr_remote_analysis.py` — Remote vs On-site Analysis
**Mục tiêu:** Phân tích xu hướng WFH hậu Covid-19 trong ngành IT.
**Quy trình MapReduce:**
- **MAP:** Trích xuất cờ (Flag) `Remote` hoặc `On-site`.
- **REDUCE 1:** Đếm tỷ trọng Remote vs On-site.
- **REDUCE 2:** Chạy lệnh PIVOT qua Spark SQL để bẻ bảng: So sánh lương Remote và Onsite ở CÙNG MỘT CẤP BẬC.
- **Output:** `data/parquet/remote_analysis/`

### 📄 8. `mr_job_level_distribution.py` — Job Level Distribution
**Mục tiêu:** Đánh giá chất lượng của các trang tuyển dụng (TopCV, ITviec, VietnamWorks) thiên về cấp bậc nào.
**Quy trình MapReduce:**
- **MAP:** Map cặp `(source, job_level)`.
- **REDUCE (PIVOT):** Xoay trục `groupBy(source).pivot(job_level).count()` để tạo ma trận so sánh các trang web theo từng Level (Ai chuyên Fresher, ai chuyên Senior).
- **Output:** `data/parquet/job_level_distribution/`

### 📄 9. `mr_skill_cooccurrence.py` — Market Basket Analysis
**Mục tiêu:** Tìm ra các "cặp bài trùng" kỹ năng thường đi chung với nhau (VD: Java-SQL).
**Quy trình MapReduce:**
- **MAP:** Explode mảng kỹ năng và sinh ra tất cả các cặp kết hợp chéo (Combinations) của từng Job.
- **REDUCE:** Tổng hợp đếm và lấy Top 50 cặp xuất hiện nhiều nhất.
- **Output:** `data/parquet/skill_cooccurrence/`

### 📄 10. `mr_top_paying_jobs.py` — Top-N Window
**Mục tiêu:** Lọc đích danh 3 công việc lương cao nhất cho TỪNG CẤP BẬC.
**Quy trình MapReduce:**
- **MAP:** Chọn cột level, công ty, chức danh, lương.
- **REDUCE:** Sử dụng `Window.partitionBy(level).orderBy(salary)` và `row_number() <= 3` để gọt bớt dữ liệu, chỉ giữ Top 3 mỗi lát cắt.
- **Output:** `data/parquet/top_paying_jobs/`

### 📄 11. `mr_cross_tabulation.py` — Pivot Cross-Tabulation
**Mục tiêu:** Vẽ ma trận Lương trung bình theo Kỹ Năng x Địa Điểm (HCM vs HN vs ĐN).
**Quy trình MapReduce:**
- **MAP:** Lọc bỏ dữ liệu rác, bóc tách kỹ năng `explode()`.
- **REDUCE:** Xoay trục Pivot: `groupBy(skill).pivot(location).avg(salary)`.
- **Output:** `data/parquet/cross_tabulation/`

### 📄 12. `mr_salary_bracketing.py` — Salary Histogram
**Mục tiêu:** Gom cụm phổ lương toàn thị trường thành các nhóm (Histogram) để vẽ biểu đồ phân phối.
**Quy trình MapReduce:**
- **MAP:** Sử dụng logic phân nhánh `F.when` để tự động ném mức lương vào các xô (Buckets: <15M, 15-30M, 30-50M, >50M).
- **REDUCE:** Đếm số lượng tin tuyển dụng trong từng xô (`count`).
- **Output:** `data/parquet/salary_bracketing/`

### 📄 13. `mr_skill_rarity.py` — TF-IDF from scratch
**Mục tiêu:** Đo lường độ hiếm và đặc thù của kỹ năng (Kỹ năng hiếm sẽ có điểm số cao).
**Quy trình MapReduce:**
- **MAP:** Bóc tách từ khóa. Lọc bỏ trùng lặp trong nội bộ 1 job (để tính Document Frequency).
- **REDUCE:** Đếm tổng số job xuất hiện kỹ năng đó (DF). Áp dụng công thức IDF của Machine Learning: `Log(N / DF)`.
- **Output:** `data/parquet/skill_rarity/`

### 📄 14. `mr_salary_outliers.py` — Anomaly Outlier Detection
**Mục tiêu:** Dò tìm bất thường: Cty nào đang "phá giá" thị trường (trả lương cao đột biến so với mặt bằng).
**Quy trình MapReduce:**
- **MAP:** Tính Q1 (Percentile 25) và Q3 (Percentile 75) cho từng cấp bậc.
- **REDUCE:** Join lại bảng gốc, áp dụng công thức `Upper_Bound = Q3 + 1.5 * IQR` để lọc ra các job nằm ngoài khoảng phân phối chuẩn (Outliers).
- **Output:** `data/parquet/salary_outliers/`

### 📄 15. `mr_olap_cube.py` — OLAP Cube Data Warehousing
**Mục tiêu:** Tính toán sẵn các Sub-totals và Grand-totals cho 3 chiều: `Địa Điểm x Cấp Bậc x Remote`.
**Quy trình MapReduce:**
- **MAP:** Lọc và chuẩn hóa dữ liệu Địa Điểm (HCM/HN), Cấp bậc, Trạng thái Remote.
- **REDUCE:** Sử dụng lệnh `.cube()` thần thánh để tự động nảy sinh 8 tổ hợp gom nhóm khác nhau, tính Lương trung bình và Số job.
- **Output:** `data/parquet/olap_cube/`

---

## ▶️ Hướng Dẫn Vận Hành (How to run)

Do hệ thống đã cấu hình chạy local, chỉ cần mở Terminal tại thư mục gốc và chạy lệnh `spark-submit`:

```bash
spark-submit spark/mr_location.py
spark-submit spark/mr_top_skills.py
spark-submit spark/mr_salary_by_level.py
spark-submit spark/mr_salary_by_skill.py
spark-submit spark/mr_yoe_salary_correlation.py
spark-submit spark/mr_company_hiring.py
spark-submit spark/mr_remote_analysis.py
spark-submit spark/mr_job_level_distribution.py
spark-submit spark/mr_skill_cooccurrence.py
spark-submit spark/mr_top_paying_jobs.py
spark-submit spark/mr_cross_tabulation.py
spark-submit spark/mr_salary_bracketing.py
spark-submit spark/mr_skill_rarity.py
spark-submit spark/mr_salary_outliers.py
spark-submit spark/mr_olap_cube.py
```

*Tip: Có thể chạy nối tiếp toàn bộ bằng lệnh PowerShell: `spark-submit spark/mr_location.py; spark-submit spark/mr_top_skills.py; ...`*
