import sys
import io
from pyspark.sql import SparkSession

# Fix Unicode output on Windows (cp1252 -> utf-8)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

spark = SparkSession.builder \
    .appName("MR_Location") \
    .getOrCreate()

df = spark.read.option("header", "true") \
    .csv("file:///D:/HDFS/JOB_MARKET_BIGDATA/data/processed/Data_ITJOB_Cleaned.csv")

result = df.groupBy("location_clean") \
           .count() \
           .orderBy("count", ascending=False)

# Ghi kết quả ra file txt (UTF-8) thay vì in ra terminal để tránh lỗi encoding
output_path = "D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/location_result.txt"
with open(output_path, "w", encoding="utf-8") as f:
    rows = result.collect()
    f.write(f"{'location_clean':<45} {'count':>6}\n")
    f.write("-" * 53 + "\n")
    for row in rows:
        f.write(f"{str(row['location_clean']):<45} {row['count']:>6}\n")

print(f"[OK] Da ghi ket qua vao: {output_path}")

result.write.mode("overwrite") \
    .csv("file:///D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/location_result")

spark.stop()