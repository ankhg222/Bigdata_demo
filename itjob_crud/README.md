# 💼 IT Job Market CRUD (Hadoop & Hive Big Data)
### Giao diện Streamlit · Xử lý PySpark · Lưu trữ Apache Hive & HDFS

> **IT Job Market CRUD** là một ứng dụng quản trị dữ liệu việc làm IT (Big Data) hoàn chỉnh với giao diện web tương tác. 
> Ứng dụng cung cấp các thao tác CRUD (Create, Read, Update, Delete) an toàn, cơ chế Soft-delete, và Quản lý Backup/Restore snapshot thông qua hệ sinh thái **Apache Spark**, **Apache Hive** và **HDFS**.

---

## ✨ Các Tính Năng Nổi Bật

### 1. 📊 Dashboard Trực Quan
- Thống kê thời gian thực: Tổng số việc làm, công ty, mức lương trung bình, và nhiều chỉ số khác.
- Biểu đồ phân tích (địa điểm, cấp độ, nguồn crawl, kỹ năng phổ biến) sử dụng dữ liệu trực tiếp từ Hive.

### 2. 🗃️ Thao tác CRUD (An Toàn Trên Hive External Tables)
Hệ thống sử dụng cơ chế xử lý Spark SQL kết hợp Temporary Views để giải quyết triệt để lỗi ghi đè dữ liệu trên Hive External Tables:
- **Read**: Phân trang, sắp xếp và lọc dữ liệu đa chiều (Theo khoảng lương, số năm kinh nghiệm, kỹ năng, địa điểm, v.v.).
- **Create**: Thêm bản ghi mới hoặc Import dữ liệu hàng loạt từ tệp CSV/HDFS.
- **Update**: Cập nhật thông tin chi tiết hoặc cập nhật hàng loạt (Bulk Update) an toàn bằng cơ chế `INSERT OVERWRITE` qua bảng ảo.
- **Delete**: Hỗ trợ **Soft-delete** (đánh dấu xóa, lưu lịch sử) và Hard-delete (xóa vĩnh viễn).

### 3. 💾 Quản Lý Sao Lưu & Phục Hồi (Backup & Restore)
- **Tạo Snapshot**: Sao lưu dữ liệu toàn bảng dưới định dạng `Parquet` lên HDFS. Metadata của các bản backup được lưu trữ trong một bảng Hive riêng biệt (`it_jobs_backup_meta`).
- **Phục hồi linh hoạt**:
  - `Replace`: Thay thế hoàn toàn dữ liệu hiện tại bằng bản sao lưu (Hệ thống tự động tạo backup phòng hờ trước khi ghi đè).
  - `Merge`: Cập nhật/thêm những dữ liệu chưa tồn tại.
  - `Append`: Nối trực tiếp dữ liệu từ bản sao lưu.
- **Tự động dọn dẹp**: Giữ tối đa số lượng phiên bản backup nhất định để tối ưu dung lượng HDFS.
- **Export Dữ Liệu**: Trích xuất dữ liệu đang hoạt động ra HDFS dưới dạng CSV, JSON hoặc Parquet.

---

## 🛠️ Yêu cầu hệ thống

| Thành phần | Phiên bản | Ghi chú |
|-----------|----------|---------|
| **Ubuntu Server** | 20.04 / 22.04 LTS | Khuyên dùng cho môi trường Big Data |
| **Java** | **8** | OpenJDK 8 (Bắt buộc cho Hadoop/Hive) |
| **Hadoop (HDFS)** | **3.3.6** | Đã cài đặt và khởi chạy |
| **Apache Hive** | 3.1.3 | Hệ quản trị cơ sở dữ liệu kho |
| **Apache Spark** | 3.5.x | Xử lý dữ liệu (Cấu hình dùng chung Hive metastore) |
| **Python** | 3.8 – 3.11 | Python environment cho Streamlit & PySpark |

---

## 📂 Cấu trúc dự án

```text
itjob_crud/
│
├── app.py                       ← Entry point của ứng dụng Streamlit
├── config.py                    ← Cấu hình hệ thống (HDFS, Spark, Hive)
├── requirements.txt             ← Các thư viện Python cần thiết
│
├── src/
│   ├── spark_client.py          ← Khởi tạo SparkSession kết nối tới Hive
│   ├── hive_schema.py           ← DDL Queries tạo DB và Bảng trong Hive
│   ├── hive_operations.py       ← Xử lý logic CRUD (Đã fix lỗi External Table)
│   ├── backup_restore.py        ← Xử lý logic Backup/Restore
│   └── pages/                   ← Các module giao diện Streamlit
│       ├── dashboard.py         
│       ├── crud_page.py         
│       ├── backup_page.py       
│       └── settings_page.py     
│
├── data/                        ← Thư mục lưu trữ CSV để Import ban đầu
├── scripts/
│   ├── deploy_ubuntu.sh         ← Script hỗ trợ triển khai trên Linux
│   └── setup.py                 ← Script khởi tạo Schema và Import data ban đầu
│
└── temp/                        ← Thư mục lưu tệp upload tạm thời
```

---

## 🚀 Hướng Dẫn Cài Đặt Và Chạy Ứng Dụng

### Bước 1: Khởi động Hadoop và Hive Metastore
Đảm bảo rằng HDFS đã chạy và Hive đã được khởi tạo schema.
```bash
# Start HDFS
$HADOOP_HOME/sbin/start-dfs.sh

# Kiểm tra (Phải thấy NameNode và DataNode)
jps
```

### Bước 2: Thiết lập thư mục trên HDFS
Khởi tạo các thư mục lưu trữ dữ liệu chính cho hệ thống:
```bash
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -mkdir -p /user/backups/it_jobs
hdfs dfs -mkdir -p /user/backups/meta
hdfs dfs -mkdir -p /user/uploads
hdfs dfs -chmod -R 777 /user/
```

### Bước 3: Cài đặt Python Dependencies
Bạn nên sử dụng môi trường ảo (virtual environment).
```bash
cd /opt/itjob_crud
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Bước 4: Khởi tạo Dữ Liệu Lần Đầu (Setup)
Chạy script tự động upload file CSV, cấu trúc Hive tables và chèn dữ liệu ban đầu.
```bash
python3 scripts/setup.py
```
> **Lưu ý:** Script `setup.py` sẽ tự động tải file từ `data/Data_ITJOB_Cleaned.csv`. Hãy đảm bảo file tồn tại trước khi chạy.

### Bước 5: Chạy Ứng Dụng Streamlit
Khởi chạy ứng dụng và truy cập thông qua trình duyệt web.
```bash
streamlit run app.py --server.port 8501 --server.address 0.0.0.0
```
Truy cập hệ thống tại: `http://<IP_UBUNTU>:8501`

*(Để chạy nền trên server, bạn có thể thiết lập systemd service thông qua `scripts/deploy_ubuntu.sh`)*

---

## 💡 Lưu Ý Về Kỹ Thuật (Dành cho Developer)

- **Tránh Lỗi Xóa Dữ Liệu Hive (External Tables):** Các thao tác `TRUNCATE TABLE` không khả dụng với External tables. Ứng dụng này sử dụng cơ chế xử lý Spark In-memory bằng cách `df.collect()` dữ liệu, tạo một `Temporary View`, và ghi đè bằng lệnh `INSERT OVERWRITE TABLE ... SELECT * FROM temp_view`. Cơ chế này loại bỏ được các lỗi khóa bảng hoặc lỗi file not found từ HDFS.
- **Data Types Configuration:** Metadata của chức năng Backup sử dụng `STRING` thay vì `BIGINT` cho ID (UUID) để tránh lỗi ép kiểu (Cannot safely cast) của Spark SQL khi cập nhật.
- **Tối ưu RAM:** Do Spark đòi hỏi lượng RAM đáng kể cho việc khởi chạy, nếu hệ thống có ít hơn 4GB RAM, bạn có thể điều chỉnh cấu hình bộ nhớ của Spark tại `config.py` xuống `1g` cho Executor và Driver.
