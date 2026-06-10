import os
os.environ["PYTHONIOENCODING"] = "utf-8"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from itertools import combinations

def get_pairs(skills_str):
    if not skills_str: return []
    skills = [s.strip().lower() for s in skills_str.split(',') if s.strip()]
    skills = sorted(list(set(skills)))
    return [f"{a} + {b}" for a, b in combinations(skills, 2)]

def main():
    spark = SparkSession.builder \
        .appName("JobMarket_Skill_Cooccurrence") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    input_csv = "file:///D:/HDFS/JOB_MARKET_BIGDATA/data/processed/Data_ITJOB_Cleaned.csv"
    output_dir = "file:///D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/skill_cooccurrence"
    output_txt = "D:/HDFS/JOB_MARKET_BIGDATA/data/parquet/skill_cooccurrence.txt"
    
    df = spark.read.csv(input_csv, header=True, inferSchema=True)
    df = df.fillna({'skills_clean': ''})
    
    # Register UDF
    pairs_udf = F.udf(get_pairs, "array<string>")
    
    # Map: Generate pairs
    mapped_df = df.select(F.explode(pairs_udf("skills_clean")).alias("skill_pair"))
    
    # Reduce: Count and sort
    reduced_df = mapped_df.groupBy("skill_pair").agg(F.count("*").alias("co_occurrence_count"))
    final_df = reduced_df.orderBy(F.desc("co_occurrence_count")).limit(50)
    
    # Write Parquet
    final_df.write.mode("overwrite").parquet(output_dir)
    
    # Write TXT
    results = final_df.collect()
    with open(output_txt, "w", encoding="utf-8") as f:
        f.write("=== MARKET BASKET: TOP 50 SKILL CO-OCCURRENCE ===\n")
        f.write("Cap ky nang nao thuong xuyen di chung voi nhau nhat?\n\n")
        for i, row in enumerate(results, 1):
            f.write(f"{i:2d}. {row['skill_pair']:<35} : {row['co_occurrence_count']} jobs\n")
            
    print(f"[OK] Đã ghi Parquet: {output_dir}")
    print(f"[OK] Đã ghi TXT: {output_txt}")
    spark.stop()

if __name__ == "__main__":
    main()
