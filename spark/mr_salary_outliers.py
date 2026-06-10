import os
os.environ["PYTHONIOENCODING"] = "utf-8"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder \
        .appName("JobMarket_Salary_Outliers") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
        
    input_csv = "file:///D:/HDFS/JOB_MARKET_BIGDATA/data/processed/Data_ITJOB_Cleaned.csv"
    output_dir = "file:///D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/salary_outliers"
    output_txt = "D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/salary_outliers.txt"
    
    df = spark.read.csv(input_csv, header=True, inferSchema=True)
    df = df.filter(F.col("salary_final_vnd").isNotNull() & F.col("job_level").isNotNull())
    df = df.withColumn("salary_final_vnd", F.col("salary_final_vnd").cast("double"))
    
    # Tính Q1 (25%), Q3 (75%) cho mỗi job_level
    stats_df = df.groupBy("job_level").agg(
        F.percentile_approx("salary_final_vnd", 0.25).alias("Q1"),
        F.percentile_approx("salary_final_vnd", 0.75).alias("Q3")
    )
    
    # Tính IQR và Bounds
    stats_df = stats_df.withColumn("IQR", F.col("Q3") - F.col("Q1"))
    stats_df = stats_df.withColumn("Upper_Bound", F.col("Q3") + 1.5 * F.col("IQR"))
    
    # Join lại bảng gốc để phát hiện Outliers
    joined_df = df.join(stats_df, on="job_level", how="inner")
    
    # Lọc những Job có lương vô lý (chỉ lấy Outliers cao)
    outliers_df = joined_df.filter(F.col("salary_final_vnd") > F.col("Upper_Bound"))
    
    outliers_df = outliers_df.select(
        "company", "title_clean", "job_level", "salary_final_vnd", "Upper_Bound"
    ).orderBy(F.desc("salary_final_vnd"))
    
    # Format
    final_df = outliers_df.withColumn("Salary_M", F.round(F.col("salary_final_vnd")/1000000, 1)) \
                          .withColumn("Limit_M", F.round(F.col("Upper_Bound")/1000000, 1))
                          
    final_df.drop("salary_final_vnd", "Upper_Bound").write.mode("overwrite").parquet(output_dir)
    
    results = final_df.collect()
    with open(output_txt, "w", encoding="utf-8") as f:
        f.write("=== PHAT HIEN BAT THUONG: LUONG CAO DOT BIEN (OUTLIERS) ===\n")
        f.write("(Nhung job tra luong vuot qua nguong tran - Upper Bound cua thi truong)\n\n")
        for row in results[:50]:
            f.write(f"[{row['job_level']:<10}] {row['Salary_M']:>5.1f}M (Tran: {row['Limit_M']:>5.1f}M) | Cty: {row['company'][:25]:<25} | {row['title_clean'][:35]}\n")
            
    print(f"[OK] Da ghi Parquet: {output_dir}")
    spark.stop()

if __name__ == "__main__":
    main()
