"""
MapReduce Job 3: Salary Analysis by Job Level
===============================================
Phuong phap MapReduce:
  - MAP   : Select (job_level, salary) -> emit tung cap -> value la salary
  - REDUCE: groupBy(job_level).agg(min, avg, percentile, max, count)
  - SORT  : orderBy avg_salary DESC
Input  : Data_ITJOB_Cleaned.csv
Output : data/parquet/salary_by_level/ + salary_by_level.txt
"""

import os
os.environ["PYTHONIOENCODING"] = "utf-8"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("MR_SalaryByLevel") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

DATA_PATH  = "file:///D:/HDFS/JOB_MARKET_BIGDATA/data/processed/Data_ITJOB_Cleaned.csv"
OUTPUT_CSV = "file:///D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/salary_by_level"
OUTPUT_TXT = "D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/salary_by_level.txt"

# ── Doc & lam sach du lieu ────────────────────
df = spark.read.option("header", "true").csv(DATA_PATH)

# MAP: Chon (job_level, salary) - cast sang dung kieu
df_mapped = df \
    .filter(F.col("job_level").isNotNull() & (F.col("job_level") != "")) \
    .withColumn("salary", F.col("salary_final_vnd").cast("double")) \
    .withColumn("yoe",    F.col("yoe_extracted").cast("double")) \
    .select("job_level", "salary", "yoe")

# REDUCE: Tinh tong hop thong ke luong theo job_level
result_df = df_mapped.groupBy("job_level").agg(
    F.count("*").alias("job_count"),
    F.round(F.min(F.when(F.col("salary") > 0, F.col("salary"))) / 1e6, 1).alias("min_salary_M"),
    F.round(F.avg(F.when(F.col("salary") > 0, F.col("salary"))) / 1e6, 1).alias("avg_salary_M"),
    F.round(
        F.expr("percentile_approx(CASE WHEN salary > 0 THEN salary END, 0.5)") / 1e6, 1
    ).alias("median_salary_M"),
    F.round(F.max(F.when(F.col("salary") > 0, F.col("salary"))) / 1e6, 1).alias("max_salary_M"),
    F.round(F.avg(F.col("yoe")), 1).alias("avg_yoe")
).orderBy(F.col("avg_salary_M").desc())

# Them rank bang Window function
window_spec = Window.orderBy(F.col("avg_salary_M").desc())
result_df = result_df.withColumn("rank", F.rank().over(window_spec))

# ── Ghi CSV ──────────────────────────────────
result_df.write.mode("overwrite").option("header", "true").csv(OUTPUT_CSV)
print("[OK] CSV written: " + OUTPUT_CSV)

# ── Ghi TXT UTF-8 ────────────────────────────
rows = result_df.collect()
with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
    f.write("=" * 82 + "\n")
    f.write("       THONG KE LUONG THEO CAP BAC (Don vi: Trieu VND)\n")
    f.write("=" * 82 + "\n")
    hdr = (f"{'#':<4} {'Cap bac':<14} {'So job':>7} {'Min':>7} "
           f"{'TB':>8} {'Median':>8} {'Max':>7} {'Avg YOE':>8}")
    f.write(hdr + "\n")
    f.write("-" * 82 + "\n")
    for row in rows:
        avg = f"{float(row['avg_salary_M']):.1f}" if row['avg_salary_M'] else "N/A"
        med = f"{float(row['median_salary_M']):.1f}" if row['median_salary_M'] else "N/A"
        mn  = f"{float(row['min_salary_M']):.1f}"   if row['min_salary_M']  else "N/A"
        mx  = f"{float(row['max_salary_M']):.1f}"   if row['max_salary_M']  else "N/A"
        yoe = f"{float(row['avg_yoe']):.1f}"         if row['avg_yoe']       else "N/A"
        f.write(
            f"{row['rank']:<4} {str(row['job_level']):<14} {row['job_count']:>7} "
            f"{mn:>7} {avg:>8} {med:>8} {mx:>7} {yoe:>8}\n"
        )
    f.write("=" * 82 + "\n")

print("[OK] TXT written: " + OUTPUT_TXT)
spark.stop()
