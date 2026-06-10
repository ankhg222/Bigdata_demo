"""
MapReduce Job 7: Remote vs On-site Analysis
=============================================
Phuong phap MapReduce:
  - MAP   : Gan nhan "Remote"/"On-site" vao moi job theo is_remote
  - REDUCE: groupBy(work_type).agg(count, avg_salary, percentile)
  - PIVOT : Dem Remote/On-site theo tung job_level
  - SQL   : Dung Spark SQL TempView + CASE WHEN
Input  : Data_ITJOB_Cleaned.csv
Output : data/parquet/remote_analysis/ + remote_analysis.txt
"""

import os
os.environ["PYTHONIOENCODING"] = "utf-8"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MONGO_URI = (
    "mongodb+srv://khangnguyen2x0_db_user:khangnguyen2x0_db_user"
    "@cluster0.yyrcrds.mongodb.net/"
)
MONGO_DB         = "BigDataJobMarket"
MONGO_COL_INPUT  = "Jobs"
MONGO_COL_OUTPUT = "remote_analysis"

OUTPUT_TXT = "D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/remote_analysis.txt"

spark = SparkSession.builder \
    .appName("MR_RemoteAnalysis") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.mongodb.read.connection.uri",  MONGO_URI) \
    .config("spark.mongodb.write.connection.uri", MONGO_URI) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Doc du lieu ───────────────────────────────
print(f"[INFO] Reading from MongoDB: {MONGO_DB}.{MONGO_COL_INPUT}")
df = spark.read \
    .format("mongodb") \
    .option("database",   MONGO_DB) \
    .option("collection", MONGO_COL_INPUT) \
    .load()

df_clean = df \
    .withColumn("salary",    F.col("salary_final_vnd").cast("double")) \
    .withColumn("skill_cnt", F.col("skill_count").cast("int")) \
    .withColumn("yoe",       F.col("yoe_extracted").cast("double")) \
    .withColumn("is_remote_int", F.col("is_remote").cast("int"))

# MAP: Gan nhan work_type cho tung job
df_labeled = df_clean.withColumn(
    "work_type",
    F.when(F.col("is_remote_int") == 1, "Remote").otherwise("On-site")
)

total = df_labeled.count()

# Dang ky TempView
df_labeled.createOrReplaceTempView("remote_jobs")

# ── REDUCE 1: Tong quan Remote vs On-site ──
overview_df = spark.sql("""
    SELECT
        work_type,
        COUNT(*)                                                      AS job_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1)            AS pct_total,
        ROUND(AVG(CASE WHEN salary > 0 THEN salary END) / 1e6, 1)    AS avg_salary_M,
        ROUND(PERCENTILE_APPROX(CASE WHEN salary > 0 THEN salary END, 0.5) / 1e6, 1)
                                                                      AS median_salary_M,
        ROUND(AVG(skill_cnt), 1)                                      AS avg_skills,
        ROUND(AVG(yoe), 1)                                            AS avg_yoe
    FROM remote_jobs
    GROUP BY work_type
    ORDER BY job_count DESC
""")

# ── REDUCE 2: Breakdown theo cap bac (PIVOT-style) ──
level_df = spark.sql("""
    SELECT
        job_level,
        SUM(CASE WHEN work_type = 'Remote'  THEN 1 ELSE 0 END)  AS remote_count,
        SUM(CASE WHEN work_type = 'On-site' THEN 1 ELSE 0 END)  AS onsite_count,
        ROUND(AVG(CASE WHEN work_type='Remote'  AND salary>0 THEN salary END)/1e6, 1)
                                                                  AS remote_avg_M,
        ROUND(AVG(CASE WHEN work_type='On-site' AND salary>0 THEN salary END)/1e6, 1)
                                                                  AS onsite_avg_M
    FROM remote_jobs
    WHERE job_level IS NOT NULL AND job_level != ''
    GROUP BY job_level
    ORDER BY (remote_count + onsite_count) DESC
""")

# ── MAP + REDUCE 3: Top 10 skills trong job Remote ──
remote_skills_df = df_labeled \
    .filter((F.col("work_type") == "Remote") & F.col("skills_clean").isNotNull()) \
    .withColumn("skill", F.explode(F.split(F.trim(F.col("skills_clean")), ","))) \
    .withColumn("skill", F.lower(F.trim(F.col("skill")))) \
    .filter(F.col("skill") != "") \
    .groupBy("skill").agg(F.count("*").alias("count")) \
    .orderBy(F.col("count").desc()) \
    .limit(10)

# ── Ghi vao MongoDB ──────────────────────────────────
print(f"[INFO] Writing results to MongoDB: {MONGO_DB}.{MONGO_COL_OUTPUT}")
overview_df.write \
    .format("mongodb") \
    .option("database",   MONGO_DB) \
    .option("collection", MONGO_COL_OUTPUT) \
    .mode("overwrite") \
    .save()
print(f"[OK] MongoDB written: {MONGO_DB}.{MONGO_COL_OUTPUT}")

# ── Ghi TXT UTF-8 ────────────────────────────
ov_rows    = overview_df.collect()
lvl_rows   = level_df.collect()
skill_rows = remote_skills_df.collect()

with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
    f.write("=" * 70 + "\n")
    f.write("   PHAN TICH REMOTE vs ON-SITE - THI TRUONG IT VIET NAM\n")
    f.write("=" * 70 + "\n\n")

    f.write("-- TONG QUAN --\n")
    f.write(f"{'Loai':<12} {'So job':>7} {'Ti le':>7} {'Avg(M)':>8} {'Median(M)':>10} {'Avg Skills':>11} {'Avg YOE':>8}\n")
    f.write("-" * 70 + "\n")
    for row in ov_rows:
        avg = f"{float(row['avg_salary_M']):.1f}" if row['avg_salary_M'] else "N/A"
        med = f"{float(row['median_salary_M']):.1f}" if row['median_salary_M'] else "N/A"
        f.write(
            f"{str(row['work_type']):<12} {row['job_count']:>7} "
            f"{float(row['pct_total']):>6.1f}% {avg:>8} {med:>10} "
            f"{float(row['avg_skills']):>11.1f} {float(row['avg_yoe']):>8.1f}\n"
        )

    f.write("\n-- BREAKDOWN THEO CAP BAC --\n")
    f.write(f"{'Cap bac':<15} {'Remote':>8} {'On-site':>9} {'AvgRemote(M)':>13} {'AvgOnsite(M)':>13}\n")
    f.write("-" * 62 + "\n")
    for row in lvl_rows:
        ra = f"{float(row['remote_avg_M']):.1f}" if row['remote_avg_M'] else "N/A"
        oa = f"{float(row['onsite_avg_M']):.1f}" if row['onsite_avg_M'] else "N/A"
        f.write(f"{str(row['job_level']):<15} {row['remote_count']:>8} {row['onsite_count']:>9} "
                f"{ra:>13} {oa:>13}\n")

    f.write("\n-- TOP 10 KY NANG PHO BIEN TRONG JOB REMOTE --\n")
    for i, row in enumerate(skill_rows, 1):
        f.write(f"  {i:>2}. {str(row['skill']):<28} {row['count']} jobs\n")

    f.write(f"\nTong job phan tich: {total}\n")

print("[OK] TXT written: " + OUTPUT_TXT)

# ── Upload len HDFS ──────────────────────────────────────────────────────────
HDFS_OUTPUT_DIR = "/project/output/"
print(f"[INFO] Uploading TXT to HDFS: {HDFS_OUTPUT_DIR}")
os.system(f"hdfs dfs -mkdir -p {HDFS_OUTPUT_DIR}")
os.system(f"hdfs dfs -put -f {OUTPUT_TXT} {HDFS_OUTPUT_DIR}")
print("[OK] HDFS upload command executed.")

spark.stop()
