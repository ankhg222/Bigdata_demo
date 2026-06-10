import os
os.environ["PYTHONIOENCODING"] = "utf-8"
import math

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder \
        .appName("JobMarket_Skill_Rarity") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
        
    input_csv = "file:///D:/HDFS/JOB_MARKET_BIGDATA/data/processed/Data_ITJOB_Cleaned.csv"
    output_dir = "file:///D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/skill_rarity"
    output_txt = "D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/skill_rarity.txt"
    
    df = spark.read.csv(input_csv, header=True, inferSchema=True)
    df = df.filter(F.col("skills_clean").isNotNull())
    
    total_documents = df.count()
    
    # Map: Explode skills, distinct per job so we get Document Frequency (DF)
    # We use "title_clean" and "company" to uniquely identify a job in case of missing IDs
    mapped_df = df.select("title_clean", "company", F.explode(F.split(F.lower(F.col("skills_clean")), ",")).alias("skill"))
    mapped_df = mapped_df.withColumn("skill", F.trim(F.col("skill"))).filter(F.col("skill") != "")
    
    # Drop duplicates per job so DF is 1 per job
    df_unique = mapped_df.dropDuplicates(["title_clean", "company", "skill"])
    
    # Reduce: Calculate DF
    reduced_df = df_unique.groupBy("skill").agg(F.count("*").alias("doc_freq"))
    
    # Filter outliers (skills appearing less than 5 times)
    reduced_df = reduced_df.filter(F.col("doc_freq") >= 5)
    
    # Calculate IDF (Inverse Document Frequency): log(N / DF)
    final_df = reduced_df.withColumn("idf_score", F.round(F.log(total_documents / F.col("doc_freq")), 4))
    
    # Sort by IDF descending (Highest IDF = Rarest Skills)
    final_df = final_df.orderBy(F.desc("idf_score")).limit(50)
    
    final_df.write.mode("overwrite").parquet(output_dir)
    
    results = final_df.collect()
    with open(output_txt, "w", encoding="utf-8") as f:
        f.write("=== SKILL RARITY (IDF SCORE) ===\n")
        f.write(f"Tong so job (N) = {total_documents}\n")
        f.write("(Diem IDF cao nhat -> Ky nang hiem, dac thu nhat thi truong)\n\n")
        f.write(f"{'SKILL':<25} | {'SO JOB XUAT HIEN (DF)':<25} | {'DIEM DO HIEM (IDF)':<20}\n")
        f.write("-" * 75 + "\n")
        for i, row in enumerate(results, 1):
            f.write(f"{i:2d}. {row['skill']:<21} | {row['doc_freq']:<25} | {row['idf_score']}\n")
            
    print(f"[OK] Đã ghi Parquet: {output_dir}")
    spark.stop()

if __name__ == "__main__":
    main()
