"""
MapReduce Job 2: Top Skills Analysis
=====================================
Phuong phap MapReduce:
  - MAP   : Explode cot skills_clean -> moi skill la 1 record (key=skill, value=1)
  - REDUCE: groupBy(skill).agg(count) -> tong so job yeu cau moi skill
  - SORT  : orderBy(count DESC) -> xep hang
Input  : Data_ITJOB_Cleaned.csv
Output : data/parquet/top_skills/ (CSV) + data/parquet/top_skills.txt
"""

import os
os.environ["PYTHONIOENCODING"] = "utf-8"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("MR_TopSkills") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

DATA_PATH  = "file:///D:/HDFS/JOB_MARKET_BIGDATA/data/processed/Data_ITJOB_Cleaned.csv"
OUTPUT_CSV = "file:///D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/top_skills"
OUTPUT_TXT = "D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/top_skills.txt"

# ── Doc du lieu ──────────────────────────────
df = spark.read.option("header", "true").csv(DATA_PATH)
total_jobs = df.count()
print(f"[INFO] Total jobs: {total_jobs}")

# ── MAP: Tach tung skill thanh 1 row rieng biet ──
skills_mapped = df \
    .filter(F.col("skills_clean").isNotNull() & (F.col("skills_clean") != "")) \
    .withColumn("skill", F.explode(F.split(F.trim(F.col("skills_clean")), ","))) \
    .withColumn("skill", F.lower(F.trim(F.col("skill")))) \
    .filter(F.col("skill") != "") \
    .select("skill")

# ── REDUCE: Dem so lan xuat hien moi skill (sum of 1s) ──
skill_counts = skills_mapped \
    .groupBy("skill") \
    .agg(F.count("*").alias("job_count")) \
    .withColumn("pct_jobs", F.round(F.col("job_count") * 100.0 / total_jobs, 2)) \
    .orderBy(F.col("job_count").desc()) \
    .limit(20)

# ── Ghi CSV ──────────────────────────────────
skill_counts.write.mode("overwrite").option("header", "true").csv(OUTPUT_CSV)
print("[OK] CSV written: " + OUTPUT_CSV)

# ── Ghi TXT UTF-8 ────────────────────────────
rows = skill_counts.collect()
with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
    f.write("=" * 60 + "\n")
    f.write("  TOP 20 KY NANG IT DUOC YEU CAU NHIEU NHAT\n")
    f.write("=" * 60 + "\n")
    f.write(f"  Tong so job phan tich: {total_jobs}\n")
    f.write("=" * 60 + "\n")
    f.write(f"{'Rank':<5} {'Ky nang':<30} {'So job':>8} {'Ti le (%)':>10}\n")
    f.write("-" * 60 + "\n")
    for i, row in enumerate(rows, 1):
        f.write(f"{i:<5} {str(row['skill']):<30} {row['job_count']:>8} {float(row['pct_jobs']):>9.1f}%\n")
    f.write("=" * 60 + "\n")

print("[OK] TXT written: " + OUTPUT_TXT)
spark.stop()
