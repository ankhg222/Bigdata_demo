import os
os.environ["PYTHONIOENCODING"] = "utf-8"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder \
        .appName("JobMarket_Salary_Bracketing") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
        
    input_csv = "file:///D:/HDFS/JOB_MARKET_BIGDATA/data/processed/Data_ITJOB_Cleaned.csv"
    output_dir = "file:///D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/salary_bracketing"
    output_txt = "D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/salary_bracketing.txt"
    
    df = spark.read.csv(input_csv, header=True, inferSchema=True)
    df = df.withColumn("salary_final_vnd", F.col("salary_final_vnd").cast("double"))
    
    # Map: Bucketing using F.when
    mapped_df = df.filter(F.col("salary_final_vnd").isNotNull()).withColumn(
        "salary_bucket",
        F.when(F.col("salary_final_vnd") < 15000000, "1. < 15 Triệu")
         .when((F.col("salary_final_vnd") >= 15000000) & (F.col("salary_final_vnd") < 30000000), "2. 15-30 Triệu")
         .when((F.col("salary_final_vnd") >= 30000000) & (F.col("salary_final_vnd") < 50000000), "3. 30-50 Triệu")
         .when((F.col("salary_final_vnd") >= 50000000) & (F.col("salary_final_vnd") < 80000000), "4. 50-80 Triệu")
         .otherwise("5. > 80 Triệu")
    )
    
    # Reduce: Count
    reduced_df = mapped_df.groupBy("salary_bucket").agg(F.count("*").alias("job_count"))
    
    # Sort by bucket name
    final_df = reduced_df.orderBy("salary_bucket")
    
    final_df.write.mode("overwrite").parquet(output_dir)
    
    results = final_df.collect()
    total_jobs = sum([r['job_count'] for r in results])
    
    with open(output_txt, "w", encoding="utf-8") as f:
        f.write("=== SALARY HISTOGRAM BRACKETING ===\n\n")
        for row in results:
            pct = (row['job_count'] / total_jobs) * 100
            bar = "█" * int(pct / 2)
            f.write(f"{row['salary_bucket']:<15} | {row['job_count']:>4} jobs ({pct:>5.1f}%) | {bar}\n")
            
    print(f"[OK] Đã ghi Parquet: {output_dir}")
    spark.stop()

if __name__ == "__main__":
    main()
