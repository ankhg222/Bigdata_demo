MongoDB (BigDataJobMarket.Jobs)
        │
        │  spark.read.format("mongodb")
        ▼
   [Spark xử lý]
   MAP   → filter company, cast salary
   REDUCE→ groupBy(company).agg(count, avg, min, max, collect_set)
   SORT  → top 20 theo job_count
   WINDOW→ thêm cột rank
        │
        ├─► spark.write.format("mongodb")  ──► MongoDB (BigDataJobMarket.company_hiring)
        │
        └─► open(OUTPUT_TXT, "w")          ──► company_hiring.txt (backup local)
