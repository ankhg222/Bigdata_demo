# 🗂️ MapReduce Jobs — Thị Trường IT Việt Nam

Thư mục này chứa các **PySpark MapReduce job** phân tích dữ liệu tuyển dụng IT Việt Nam.  
Tất cả các job đều đọc từ cùng 1 file đầu vào và ghi kết quả ra thư mục `data/parquet/`.

---

## 📥 Dữ liệu đầu vào (chung)

```
D:/HDFS/JOB_MARKET_BIGDATA/data/processed/Data_ITJOB_Cleaned.csv
```

---

## 📋 Danh sách các MapReduce Job

| # | File | Tên Job | Mô tả ngắn |
|---|------|---------|------------|
| 2 | `mr_top_skills.py` | Top Skills Analysis | Top 20 kỹ năng IT được yêu cầu nhiều nhất |
| 3 | `mr_salary_by_level.py` | Salary by Job Level | Thống kê lương theo cấp bậc công việc |
| 4 | `mr_salary_by_skill.py` | Salary by Skill | Top 15 kỹ năng có mức lương cao nhất |
| 5 | `mr_yoe_salary_correlation.py` | YOE vs Salary | Tương quan năm kinh nghiệm và lương |
| 6 | `mr_company_hiring.py` | Company Hiring | Top 20 công ty tuyển dụng IT nhiều nhất |
| 7 | `mr_remote_analysis.py` | Remote vs On-site | Phân tích xu hướng làm việc Remote/On-site |
| 8 | `mr_job_level_distribution.py` | Job Level Distribution | Phân bố cấp bậc theo nguồn tuyển dụng |
| — | `mr_location.py` | Location Analysis | Thống kê số lượng job theo địa điểm |

---

## 🔍 Chi tiết từng file

---

### 📄 `mr_top_skills.py` — MapReduce Job 2: Top Skills Analysis

**Mục tiêu:** Xác định top 20 kỹ năng IT được yêu cầu nhiều nhất trên thị trường.

**Phương pháp MapReduce:**
- **MAP:** Explode cột `skills_clean` → mỗi skill thành 1 record độc lập `(skill, 1)`
- **REDUCE:** `groupBy(skill).agg(count)` → tổng số job yêu cầu mỗi skill
- **SORT:** `orderBy(count DESC)` → lấy top 20

**Cột đầu ra:**

| Cột | Mô tả |
|-----|-------|
| `skill` | Tên kỹ năng (lowercase) |
| `job_count` | Số lượng job yêu cầu skill này |
| `pct_jobs` | Tỷ lệ % trên tổng số job |

**Output:**
```
data/parquet/top_skills/          ← CSV (Parquet folder)
data/parquet/top_skills.txt       ← Bảng kết quả định dạng text
```

---

### 📄 `mr_salary_by_level.py` — MapReduce Job 3: Salary by Job Level

**Mục tiêu:** Thống kê mức lương (min/avg/median/max) theo từng cấp bậc công việc.

**Phương pháp MapReduce:**
- **MAP:** Select `(job_level, salary_final_vnd, yoe_extracted)` → cast sang đúng kiểu
- **REDUCE:** `groupBy(job_level).agg(min, avg, percentile_approx, max, count)`
- **SORT:** `orderBy(avg_salary DESC)`
- **WINDOW:** Thêm cột `rank` bằng `Window.rank()`

**Cột đầu ra:**

| Cột | Mô tả |
|-----|-------|
| `job_level` | Cấp bậc (Fresher, Junior, Middle, Senior, ...) |
| `job_count` | Tổng số job |
| `min_salary_M` | Lương tối thiểu (triệu VND) |
| `avg_salary_M` | Lương trung bình (triệu VND) |
| `median_salary_M` | Lương median (triệu VND) |
| `max_salary_M` | Lương tối đa (triệu VND) |
| `avg_yoe` | Số năm kinh nghiệm trung bình |
| `rank` | Xếp hạng theo lương TB |

**Output:**
```
data/parquet/salary_by_level/     ← CSV
data/parquet/salary_by_level.txt  ← Bảng kết quả định dạng text
```

---

### 📄 `mr_salary_by_skill.py` — MapReduce Job 4: Salary by Skill

**Mục tiêu:** Tìm top 15 kỹ năng kỹ thuật có mức lương trung bình cao nhất.

**Phương pháp MapReduce:**
- **MAP:** Explode `skills_clean` → emit `(skill, salary)` cho từng cặp
- **REDUCE:** `groupBy(skill).agg(count, avg, percentile_approx, min, max)` trên salary
- **FILTER:** Chỉ giữ skill xuất hiện **≥ 10 job** (đủ ý nghĩa thống kê)
- **SORT:** `orderBy(avg_salary DESC)` → lấy top 15

**Cột đầu ra:**

| Cột | Mô tả |
|-----|-------|
| `skill` | Tên kỹ năng |
| `job_count` | Số job yêu cầu skill này |
| `avg_salary_M` | Lương trung bình (triệu VND) |
| `min_salary_M` | Lương thấp nhất (triệu VND) |
| `median_salary_M` | Lương median (triệu VND) |
| `max_salary_M` | Lương cao nhất (triệu VND) |

**Output:**
```
data/parquet/salary_by_skill/     ← CSV
data/parquet/salary_by_skill.txt  ← Bảng kết quả định dạng text
```

---

### 📄 `mr_yoe_salary_correlation.py` — MapReduce Job 5: YOE vs Salary Correlation

**Mục tiêu:** Phân tích tương quan giữa số năm kinh nghiệm (YOE) và mức lương.

**Phương pháp MapReduce:**
- **MAP:** Gán mỗi job vào 1 bucket kinh nghiệm theo `yoe_extracted`:
  - `00-01yr (Fresher)`, `02yr (Junior)`, `03-04yr (Mid-level)`, `05-06yr (Senior)`, `07-09yr (Lead)`, `10+yr (Expert/Mgr)`
- **REDUCE:** Dùng **Spark SQL TempView** → `groupBy(yoe_bucket).agg(count, avg, percentile, min, max)`
- **WINDOW:** Tính mức tăng lương so với nhóm trước bằng hàm `LAG()` trên Window

**Cột đầu ra:**

| Cột | Mô tả |
|-----|-------|
| `yoe_bucket` | Nhóm kinh nghiệm |
| `job_count` | Số job trong nhóm |
| `avg_yoe` | YOE trung bình thực tế |
| `min/avg/median/max_salary_M` | Các mốc lương (triệu VND) |
| `avg_skill_count` | Số kỹ năng trung bình yêu cầu |
| `salary_increase_M` | Mức tăng lương so với nhóm trước (triệu VND) |

**Output:**
```
data/parquet/yoe_salary_correlation/     ← CSV
data/parquet/yoe_salary_correlation.txt  ← Bảng kết quả định dạng text
```

---

### 📄 `mr_company_hiring.py` — MapReduce Job 6: Company Hiring Analysis

**Mục tiêu:** Xếp hạng top 20 công ty tuyển dụng IT nhiều nhất và phân tích chính sách lương.

**Phương pháp MapReduce:**
- **MAP:** Select `(company, salary, job_level, location_clean)` → 1 record mỗi job
- **REDUCE:** `groupBy(company).agg(count, avg_salary, min_salary, max_salary, collect_set(job_level))`
- **SORT:** `orderBy(job_count DESC)` → top 20
- **EXTRA:** Tính `market_share_pct (%)` bằng Spark broadcast `total_jobs`

**Cột đầu ra:**

| Cột | Mô tả |
|-----|-------|
| `rank` | Xếp hạng |
| `company` | Tên công ty |
| `job_count` | Tổng số job đang tuyển |
| `market_share_pct` | Thị phần tuyển dụng (%) |
| `avg_salary_M` | Lương trung bình (triệu VND) |
| `min_salary_M` | Lương thấp nhất (triệu VND) |
| `max_salary_M` | Lương cao nhất (triệu VND) |
| `levels_hired` | Các cấp bậc đang tuyển |

**Output:**
```
data/parquet/company_hiring/     ← CSV
data/parquet/company_hiring.txt  ← Bảng kết quả định dạng text
```

---

### 📄 `mr_remote_analysis.py` — MapReduce Job 7: Remote vs On-site Analysis

**Mục tiêu:** Phân tích xu hướng Remote/On-site: tỷ lệ, mức lương, kỹ năng phổ biến.

**Phương pháp MapReduce:**
- **MAP:** Gán nhãn `"Remote"` / `"On-site"` cho mỗi job dựa trên cột `is_remote`
- **REDUCE 1:** `groupBy(work_type).agg(count, avg_salary, median_salary, avg_skills, avg_yoe)` qua Spark SQL
- **REDUCE 2:** Pivot-style bằng `CASE WHEN` → breakdown Remote/On-site theo từng `job_level`
- **REDUCE 3:** MAP + REDUCE → Top 10 skill phổ biến trong job Remote

**Cột đầu ra chính:**

| Cột | Mô tả |
|-----|-------|
| `work_type` | Remote / On-site |
| `job_count` | Số lượng job |
| `pct_total` | Tỷ lệ % tổng |
| `avg/median_salary_M` | Lương TB và median (triệu VND) |
| `avg_skills` | Số kỹ năng TB yêu cầu |
| `avg_yoe` | Năm kinh nghiệm TB |

**Output:**
```
data/parquet/remote_analysis/     ← CSV
data/parquet/remote_analysis.txt  ← Bảng kết quả định dạng text (3 phần)
```

---

### 📄 `mr_job_level_distribution.py` — MapReduce Job 8: Job Level Distribution

**Mục tiêu:** Phân tích phân bố cấp bậc công việc theo từng nguồn tuyển dụng (platform).

**Phương pháp MapReduce:**
- **MAP:** Select `(source, job_level, salary, yoe, skill_count)` → cast đúng kiểu
- **REDUCE 1:** `groupBy(job_level)` tổng thể qua Spark SQL (có `OVER()` window để tính %)
- **REDUCE 2 (PIVOT):** `groupBy(source).pivot(job_level).count()` → ma trận source × level
- **REDUCE 3:** `groupBy(source, job_level)` → Top 15 cặp (nguồn, cấp bậc) nhiều job nhất

**Cột đầu ra chính (REDUCE 1):**

| Cột | Mô tả |
|-----|-------|
| `job_level` | Cấp bậc |
| `job_count` | Số lượng job |
| `pct_total` | Tỷ lệ % tổng |
| `avg/median_salary_M` | Lương TB và median (triệu VND) |
| `avg_yoe` | Năm kinh nghiệm TB |
| `avg_skills` | Số kỹ năng TB |

**Output:**
```
data/parquet/job_level_distribution/     ← CSV (REDUCE 1)
data/parquet/job_level_distribution.txt  ← 3 phần: tổng thể + pivot + top 15
```

---

### 📄 `mr_location.py` — Location Analysis

**Mục tiêu:** Đếm số lượng job theo từng địa điểm (tỉnh/thành phố).

**Phương pháp MapReduce:**
- **MAP/REDUCE:** `groupBy(location_clean).count()` → sort giảm dần

**Cột đầu ra:**

| Cột | Mô tả |
|-----|-------|
| `location_clean` | Địa điểm đã chuẩn hóa |
| `count` | Số lượng job tại địa điểm đó |

**Output:**
```
data/parquet/location_result/     ← CSV
data/parquet/location_result.txt  ← Bảng kết quả định dạng text
```

---

## 🏗️ Kiến trúc chung

```
Data_ITJOB_Cleaned.csv
        │
        ├─► mr_top_skills.py            → top_skills/
        ├─► mr_salary_by_level.py       → salary_by_level/
        ├─► mr_salary_by_skill.py       → salary_by_skill/
        ├─► mr_yoe_salary_correlation.py→ yoe_salary_correlation/
        ├─► mr_company_hiring.py        → company_hiring/
        ├─► mr_remote_analysis.py       → remote_analysis/
        ├─► mr_job_level_distribution.py→ job_level_distribution/
        └─► mr_location.py             → location_result/
```

---

## ▶️ Cách chạy

```bash
# Chạy từng job riêng lẻ
spark-submit spark/mr_top_skills.py
spark-submit spark/mr_salary_by_level.py
spark-submit spark/mr_salary_by_skill.py
spark-submit spark/mr_yoe_salary_correlation.py
spark-submit spark/mr_company_hiring.py
spark-submit spark/mr_remote_analysis.py
spark-submit spark/mr_job_level_distribution.py
spark-submit spark/mr_location.py
```

> **Lưu ý:** Mỗi job dùng `spark.sql.shuffle.partitions = 4` để tối ưu cho môi trường local/single-node.

---

## 📦 Yêu cầu

| Thành phần | Phiên bản khuyến nghị |
|------------|----------------------|
| Python | 3.8+ |
| Apache Spark | 3.x |
| PySpark | 3.x |
| Java | 8 hoặc 11 |

---

## 📁 Cấu trúc output

```
data/parquet/
├── top_skills/
├── top_skills.txt
├── salary_by_level/
├── salary_by_level.txt
├── salary_by_skill/
├── salary_by_skill.txt
├── yoe_salary_correlation/
├── yoe_salary_correlation.txt
├── company_hiring/
├── company_hiring.txt
├── remote_analysis/
├── remote_analysis.txt
├── job_level_distribution/
├── job_level_distribution.txt
├── location_result/
└── location_result.txt
```
