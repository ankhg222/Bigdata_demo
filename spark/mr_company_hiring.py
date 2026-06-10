"""
MapReduce Job 6: Company Hiring Analysis
==========================================
Phuong phap MapReduce:
  - MAP   : Select (company, salary, job_level) -> 1 record moi job
  - REDUCE: groupBy(company).agg(count, avg_salary, collect_set levels)
  - SORT  : orderBy(job_count DESC) -> top 20
  - EXTRA : Tinh market_share (%) voi Spark broadcast total_jobs
Input  : Data_ITJOB_Cleaned.csv
Output : data/parquet/company_hiring/ + company_hiring.txt
"""

import os
os.environ["PYTHONIOENCODING"] = "utf-8"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("MR_CompanyHiring") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

DATA_PATH  = "file:///D:/HDFS/JOB_MARKET_BIGDATA/data/processed/Data_ITJOB_Cleaned.csv"
OUTPUT_CSV = "file:///D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/company_hiring"
OUTPUT_TXT = "D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/company_hiring.txt"

# ── Doc du lieu ───────────────────────────────
df = spark.read.option("header", "true").csv(DATA_PATH)

# MAP: Chon cac truong can thiet, cast kieu
df_mapped = df \
    .filter(F.col("company").isNotNull() & (F.col("company") != "")) \
    .withColumn("salary",    F.col("salary_final_vnd").cast("double")) \
    .select("company", "salary", "job_level", "location_clean")

total_jobs = df_mapped.count()
print(f"[INFO] Total jobs with company: {total_jobs}")

# ── REDUCE: Tinh tong hop thong tin tuyen dung theo cong ty ──
result_df = df_mapped.groupBy("company").agg(
    F.count("*").alias("job_count"),
    F.round(F.avg(F.when(F.col("salary") > 0, F.col("salary"))) / 1e6, 1).alias("avg_salary_M"),
    F.round(F.min(F.when(F.col("salary") > 0, F.col("salary"))) / 1e6, 1).alias("min_salary_M"),
    F.round(F.max(F.when(F.col("salary") > 0, F.col("salary"))) / 1e6, 1).alias("max_salary_M"),
    F.array_join(F.collect_set("job_level"), " | ").alias("levels_hired")
) \
.withColumn("market_share_pct", F.round(F.col("job_count") * 100.0 / total_jobs, 2)) \
.orderBy(F.col("job_count").desc()) \
.limit(20)

# Them rank bang Window function
window_spec = Window.orderBy(F.col("job_count").desc())
result_df = result_df.withColumn("rank", F.rank().over(window_spec))

# ── Ghi CSV ──────────────────────────────────
result_df.select(
    "rank", "company", "job_count", "market_share_pct",
    "avg_salary_M", "min_salary_M", "max_salary_M", "levels_hired"
).write.mode("overwrite").option("header", "true").csv(OUTPUT_CSV)
print("[OK] CSV written: " + OUTPUT_CSV)

# ── Ghi TXT UTF-8 ────────────────────────────
rows = result_df.collect()
with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
    f.write("=" * 85 + "\n")
    f.write("     TOP 20 CONG TY TUYEN DUNG IT NHIEU NHAT\n")
    f.write(f"     (Tong thi truong: {total_jobs} job listings)\n")
    f.write("=" * 85 + "\n")
    hdr = f"{'#':<4} {'Cong ty':<36} {'Jobs':>5} {'Share%':>7} {'AvgLuong(M)':>12} {'Levels'}"
    f.write(hdr + "\n")
    f.write("-" * 85 + "\n")
    for row in rows:
        avg   = f"{float(row['avg_salary_M']):.1f}" if row['avg_salary_M'] else "N/A"
        levels = str(row['levels_hired'])[:28] if row['levels_hired'] else ""
        f.write(
            f"{row['rank']:<4} {str(row['company'])[:35]:<36} {row['job_count']:>5} "
            f"{float(row['market_share_pct']):>7.2f}% {avg:>11} {levels}\n"
        )
    f.write("=" * 85 + "\n")

print("[OK] TXT written: " + OUTPUT_TXT)
spark.stop()
