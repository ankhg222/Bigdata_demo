import os
os.environ["PYTHONIOENCODING"] = "utf-8"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def main():
    spark = SparkSession.builder \
        .appName("JobMarket_Top_Paying_Jobs") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
        
    input_csv = "file:///D:/HDFS/JOB_MARKET_BIGDATA/data/processed/Data_ITJOB_Cleaned.csv"
    output_dir = "file:///D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/top_paying_jobs"
    output_txt = "D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/top_paying_jobs.txt"
    
    df = spark.read.csv(input_csv, header=True, inferSchema=True)
    df = df.fillna({'job_level': 'Unknown'})
    df = df.withColumn("salary_final_vnd", F.col("salary_final_vnd").cast("double"))
    
    # Map: Select cols
    mapped_df = df.select("job_level", "company", "title_clean", "salary_final_vnd") \
                  .filter(F.col("salary_final_vnd").isNotNull())
                  
    # Reduce (Top-N): Use Window partitioning
    windowSpec = Window.partitionBy("job_level").orderBy(F.desc("salary_final_vnd"))
    
    ranked_df = mapped_df.withColumn("rank", F.row_number().over(windowSpec))
    
    # Filter top 3 per level
    final_df = ranked_df.filter(F.col("rank") <= 3).orderBy("job_level", "rank")
    
    # Format salary to Millions
    final_df = final_df.withColumn("salary_M", F.round(F.col("salary_final_vnd") / 1000000, 1))
    
    final_df.write.mode("overwrite").parquet(output_dir)
    
    results = final_df.collect()
    with open(output_txt, "w", encoding="utf-8") as f:
        f.write("=== TOP 3 CONG VIEC LUONG CAO NHAT THEO TUNG CAP BAC ===\n\n")
        current_level = ""
        for row in results:
            if row['job_level'] != current_level:
                current_level = row['job_level']
                f.write(f"\n--- {current_level.upper()} ---\n")
            f.write(f"Top {row['rank']}: {row['salary_M']} Triệu | {row['title_clean'][:40]:<40} | Cty: {row['company'][:20]}\n")
            
    print(f"[OK] Đã ghi Parquet: {output_dir}")
    spark.stop()

if __name__ == "__main__":
    main()
