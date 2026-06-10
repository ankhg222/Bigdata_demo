import os
os.environ["PYTHONIOENCODING"] = "utf-8"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder \
        .appName("JobMarket_OLAP_Cube") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
        
    input_csv = "file:///D:/HDFS/JOB_MARKET_BIGDATA/data/processed/Data_ITJOB_Cleaned.csv"
    output_dir = "file:///D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/olap_cube"
    output_txt = "D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/olap_cube.txt"
    
    df = spark.read.csv(input_csv, header=True, inferSchema=True)
    df = df.filter(F.col("salary_final_vnd").isNotNull())
    
    # Chuan hoa Location
    df = df.withColumn("loc", F.when(F.col("location_clean").like("%Hồ Chí Minh%"), "HCM")
                              .when(F.col("location_clean").like("%Hà Nội%"), "HN")
                              .otherwise("Other"))
    
    # Chuan hoa Level
    valid_levels = ['Fresher', 'Junior', 'Senior', 'Manager']
    df = df.withColumn("level", F.when(F.col("job_level").isin(valid_levels), F.col("job_level")).otherwise("Other"))
    
    # Chuan hoa Remote
    df = df.withColumn("remote_status", F.when(F.col("is_remote") == 1, "Remote").otherwise("On-site"))
    
    # OLAP Cube Aggregate
    # Cuon 3 chieu: loc, level, remote_status
    cube_df = df.cube("loc", "level", "remote_status").agg(
        F.count("*").alias("job_count"),
        F.round(F.avg("salary_final_vnd")/1000000, 1).alias("avg_salary_M")
    )
    
    # Format nulls to "ALL" to represent sub-totals and grand-totals
    cube_df = cube_df.fillna("ALL", subset=["loc", "level", "remote_status"])
    
    # Sort for readability: ALL dimensions at bottom, specifics at top
    final_df = cube_df.orderBy("loc", "level", "remote_status")
    
    final_df.write.mode("overwrite").parquet(output_dir)
    
    results = final_df.collect()
    with open(output_txt, "w", encoding="utf-8") as f:
        f.write("=== OLAP CUBE: PHAN TICH DA CHIEU (LOCATION x LEVEL x REMOTE) ===\n")
        f.write("Su dung lenh .cube() de tao ra 8 cap do Tong hop (Sub-totals & Grand-totals)\n\n")
        f.write(f"{'LOCATION':<10} | {'LEVEL':<10} | {'REMOTE':<10} | {'SO JOB':<8} | {'LUONG TB (M)'}\n")
        f.write("-" * 65 + "\n")
        for row in results:
            f.write(f"{row['loc']:<10} | {row['level']:<10} | {row['remote_status']:<10} | {row['job_count']:<8} | {row['avg_salary_M']}\n")
            
    print(f"[OK] Da ghi Parquet: {output_dir}")
    spark.stop()

if __name__ == "__main__":
    main()
