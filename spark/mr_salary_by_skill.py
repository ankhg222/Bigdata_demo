"""
MapReduce Job 4: Average Salary by Skill
==========================================
Phuong phap MapReduce:
  - MAP   : Explode skills -> (skill, salary) moi cap
  - REDUCE: groupBy(skill).agg(count, avg_salary, percentile)
  - FILTER: Chi giu skill xuat hien >= 10 jobs (du nghia thong ke)
  - SORT  : orderBy avg_salary DESC -> top 15
Input  : Data_ITJOB_Cleaned.csv
Output : data/parquet/salary_by_skill/ + salary_by_skill.txt
"""

import os
os.environ["PYTHONIOENCODING"] = "utf-8"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("MR_SalaryBySkill") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

DATA_PATH  = "file:///D:/HDFS/JOB_MARKET_BIGDATA/data/processed/Data_ITJOB_Cleaned.csv"
OUTPUT_CSV = "file:///D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/salary_by_skill"
OUTPUT_TXT = "D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/salary_by_skill.txt"

# ── Doc & lam sach ────────────────────────────
df = spark.read.option("header", "true").csv(DATA_PATH)

df_clean = df \
    .filter(
        F.col("salary_final_vnd").isNotNull() &
        (F.col("salary_final_vnd").cast("double") > 0) &
        F.col("skills_clean").isNotNull() &
        (F.col("skills_clean") != "")
    ) \
    .withColumn("salary", F.col("salary_final_vnd").cast("double"))

print(f"[INFO] Valid rows: {df_clean.count()}")

# ── MAP: Tach tung skill -> (skill, salary) ──
skills_mapped = df_clean \
    .withColumn("skill", F.explode(F.split(F.trim(F.col("skills_clean")), ","))) \
    .withColumn("skill", F.lower(F.trim(F.col("skill")))) \
    .filter(F.col("skill") != "") \
    .select("skill", "salary")

# ── REDUCE: Tinh avg/min/max/median salary theo skill ──
result_df = skills_mapped.groupBy("skill").agg(
    F.count("*").alias("job_count"),
    F.round(F.avg("salary") / 1e6, 2).alias("avg_salary_M"),
    F.round(F.min("salary") / 1e6, 2).alias("min_salary_M"),
    F.round(F.expr("percentile_approx(salary, 0.5)") / 1e6, 2).alias("median_salary_M"),
    F.round(F.max("salary") / 1e6, 2).alias("max_salary_M")
) \
.filter(F.col("job_count") >= 10) \
.orderBy(F.col("avg_salary_M").desc()) \
.limit(15)

# ── Ghi CSV ──────────────────────────────────
result_df.write.mode("overwrite").option("header", "true").csv(OUTPUT_CSV)
print("[OK] CSV written: " + OUTPUT_CSV)

# ── Ghi TXT UTF-8 ────────────────────────────
rows = result_df.collect()
with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
    f.write("=" * 72 + "\n")
    f.write("  TOP 15 KY NANG CO MUC LUONG TRUNG BINH CAO NHAT\n")
    f.write("  (Chi tinh skill xuat hien >= 10 job)\n")
    f.write("=" * 72 + "\n")
    hdr = f"{'#':<4} {'Ky nang':<28} {'So job':>7} {'Avg (M)':>8} {'Median (M)':>11} {'Max (M)':>8}"
    f.write(hdr + "\n")
    f.write("-" * 72 + "\n")
    for i, row in enumerate(rows, 1):
        f.write(
            f"{i:<4} {str(row['skill']):<28} {row['job_count']:>7} "
            f"{float(row['avg_salary_M']):>8.2f} {float(row['median_salary_M']):>11.2f} "
            f"{float(row['max_salary_M']):>8.2f}\n"
        )
    f.write("=" * 72 + "\n")

print("[OK] TXT written: " + OUTPUT_TXT)
spark.stop()
