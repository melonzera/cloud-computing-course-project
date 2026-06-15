"""
============================================================
Part 2 — A-0: 环境部署验证 (10分)
Spark on K8s 部署验证 — WordCount 入门示例
使用豆瓣电影 summary 列进行词频统计
============================================================
"""
import time
import os
import compat  # Python 3.13 + PySpark 3.4 兼容补丁
from pyspark.sql import SparkSession

CSV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                         "douban_movies.csv")

print("=" * 60)
print("A-0: Spark 环境部署验证 — WordCount")
print("=" * 60)

start_time = time.time()

# ── 创建 Spark Session ─────────────────────────────
spark = SparkSession.builder \
    .appName("WordCount-DoubanMovies") \
    .master("local[2]") \
    .getOrCreate()

sc = spark.sparkContext

print(f"\nSpark 版本: {sc.version}")
print(f"Master: {sc.master}")
print(f"App Name: {sc.appName}")

# ── 1. 从 CSV 读取 summary 列 ──────────────────────
print(f"\n[1] 从 {CSV_PATH} 加载数据...")
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("encoding", "UTF-8") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .csv(CSV_PATH)

total_movies = df.count()
print(f"加载完成: {total_movies} 部电影")

# ── 2. 使用 DataFrame 操作做 WordCount (避免 Python Worker) ──
print("\n[2] 对电影简介 (summary) 进行 WordCount (DataFrame 模式)...")

from pyspark.sql.functions import explode, split as spark_split, lower, regexp_replace, col, length, desc

# 提取 summary 列 → 分词 → explode → 清洗 → 聚合
words_df = df.select("summary") \
    .withColumn("word", explode(spark_split(regexp_replace(lower(col("summary")),
        r"[.,!?;:\"'()\[\]{}，。！？；：""''（）《》【】—…·、\n]", " "), "\\s+"))) \
    .select("word") \
    .filter(length(col("word")) >= 2)  # 过滤单字

# 聚合计数
word_counts_df = words_df.groupBy("word") \
    .count() \
    .orderBy(desc("count"))

# ── 3. 输出结果 ────────────────────────────────────
top20 = word_counts_df.limit(20).collect()

print(f"\n{'='*50}")
print(f"  WordCount 结果 — Top 20 高频词")
print(f"{'='*50}")
print(f"  {'排名':<6} {'单词':<15} {'出现次数':>10}")
print(f"  {'-'*35}")
for i, row in enumerate(top20, 1):
    print(f"  {i:<6} {row['word']:<15} {row['count']:>10}")

# 统计信息（使用 DataFrame 操作）
total_words = word_counts_df.agg({"count": "sum"}).collect()[0][0]
unique_words = word_counts_df.count()
print(f"  {'-'*35}")
print(f"  总词数: {total_words:,}")
print(f"  不重复词数: {unique_words:,}")

# ── 4. 环境验证信息 ─────────────────────────────────
elapsed = time.time() - start_time
print(f"\n{'='*50}")
print(f"  Environment Verification: PASSED")
print(f"{'='*50}")
print(f"  • Spark 版本: {sc.version}")
print(f"  • 运行模式: local[2]")
print(f"  • 数据集: douban_movies.csv ({total_movies} 部电影)")
print(f"  • 总处理时间: {elapsed:.2f}s")
print(f"  • 数据分区数: {df.rdd.getNumPartitions()}")
print(f"  {'='*50}")

spark.stop()
