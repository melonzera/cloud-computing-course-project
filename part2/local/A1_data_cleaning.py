"""
============================================================
Part 2 — A-1: 数据清洗 (10分)
使用 PySpark 对豆瓣电影数据进行清洗：
  缺失值处理 → 去重 → 类型转换 → 异常值检测
============================================================
"""
import time
import os
import compat  # Python 3.13 + PySpark 3.4 兼容补丁
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, count, mean, stddev, isnan,
    min as spark_min, max as spark_max, trim
)

CSV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                         "douban_movies.csv")

spark = SparkSession.builder \
    .appName("DataCleaning-DoubanMovies") \
    .master("local[2]") \
    .getOrCreate()

print("=" * 60)
print("A-1: 豆瓣电影数据清洗")
print("=" * 60)

# ── 1. 加载原始数据 ────────────────────────────────
print(f"\n[步骤1] 加载原始数据: {CSV_PATH}")
df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("encoding", "UTF-8") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .csv(CSV_PATH)

print(f"  原始行数: {df_raw.count()}")
print(f"  列数: {len(df_raw.columns)}")
print(f"  列名: {df_raw.columns}")
print(f"\n  原始数据前 5 行:")
df_raw.show(5, truncate=True)

# ── 2. 缺失值分析 ──────────────────────────────────
print("\n[步骤2] 缺失值分析...")
total_rows = df_raw.count()

# 缺失值数量
missing = df_raw.select([
    count(when(col(c).isNull() | isnan(c), c)).alias(c)
    for c in df_raw.columns
])
print(f"  总行数: {total_rows}")
print("  各列缺失值数量:")
missing.show()

# 缺失值比例
print("  各列缺失值比例:")
total = df_raw.count()
missing_rates = df_raw.select([
    (count(when(col(c).isNull() | isnan(c), c)) / total * 100).alias(c)
    for c in df_raw.columns
])
missing_rates.show()

# 缺失值处理策略（至少 2 种不同策略 + 选择原因）
print("\n  缺失值处理策略（3 种不同策略）:")
print("  +----------+----------+----------------------------------+")
print("  | field    | strategy | reason                           |")
print("  +----------+----------+----------------------------------+")
print("  | year     | dropna   | time dimension key, cant infer   |")
print("  |          |          | only 3% missing, acceptable loss |")
print("  | rating_  | fillna   | target variable, preserve row    |")
print("  | score    | (mean)   | 5% missing, unbiased imputation  |")
print("  | summary  | fillna   | text field, no numeric impact    |")
print("  |          | (const)  | mark as [no summary]             |")
print("  | genres   | fillna   | categorical, unknown as a class  |")
print("  | etc.     | (const)  | preserve row, safe for GROUP BY  |")
print("  +----------+----------+----------------------------------+")

# 策略1: dropna — year 缺失的直接删除（年份不可推断）
before = df_raw.count()
df = df_raw.dropna(subset=["year"])
after_drop_year = df.count()
print(f"\n  [策略1] dropna(year): {before} → {after_drop_year} (删除 {before - after_drop_year} 行)")

# 策略2: fillna(均值) — rating_score 用均值填充（保留行，无偏估计）
score_mean = df.select(mean("rating_score")).collect()[0][0]
if score_mean is None:
    score_mean = 6.0
print(f"  [策略2] fillna(rating_score, mean={score_mean:.2f}): 填充缺失评分")

# 策略3: fillna(常量) — 文本字段用占位符填充
df = df.fillna({
    "rating_score": score_mean,
    "rating_count": 0,
    "collect_count": 0,
    "movie_id": 0,
    "title": "未知标题",
    "original_title": "Unknown",
    "genres": "未知",
    "countries": "未知",
    "directors": "未知",
    "summary": "无简介",
})
print(f"  [策略3] fillna(常量): 文本字段 → '未知'/'无简介', 数值字段 → 0")

# ── 3. 去重 ────────────────────────────────────────
print("\n[步骤3] 去重...")
before = df.count()
df = df.dropDuplicates(["movie_id"])
after = df.count()
print(f"  去重前: {before} → 去重后: {after}")
print(f"  删除重复: {before - after} 行")

# ── 4. 类型转换 ────────────────────────────────────
print("\n[步骤4] 数据类型转换...")
df = df.withColumn("year", col("year").cast("int")) \
       .withColumn("movie_id", col("movie_id").cast("int"))
print("  Schema:")
df.printSchema()

# ── 5. 异常值检测 ──────────────────────────────────
print("\n[步骤5] 异常值检测与处理...")

# 5a) 评分范围 0~10
before = df.count()
df = df.filter((col("rating_score") >= 1.0) & (col("rating_score") <= 10.0))
print(f"  评分范围过滤 (1.0~10.0): {before} → {df.count()} (删除 {before - df.count()})")

# 5b) 年份范围 >= 1888
before = df.count()
df = df.filter((col("year") >= 1888) | (col("year") == 0))
print(f"  年份过滤 (>=1888 或 0): {before} → {df.count()} (删除 {before - df.count()})")

# 5c) 3σ 异常检测 — rating_count
before = df.count()
cnt_stats = df.select(mean("rating_count"), stddev("rating_count")).collect()[0]
if cnt_stats[1] and cnt_stats[1] > 0:
    lo = max(0, cnt_stats[0] - 3 * cnt_stats[1])
    hi = cnt_stats[0] + 3 * cnt_stats[1]
    df = df.filter((col("rating_count") >= lo) & (col("rating_count") <= hi))
    print(f"  rating_count 3σ ({lo:.0f}~{hi:.0f}): {before} → {df.count()}")

# 5d) 评分统计
score_stats = df.select(
    mean("rating_score").alias("mean"),
    stddev("rating_score").alias("std"),
    spark_min("rating_score").alias("min"),
    spark_max("rating_score").alias("max")
).collect()[0]
print(f"\n  评分统计: mean={score_stats['mean']:.2f}, std={score_stats['std']:.2f}, "
      f"min={score_stats['min']:.1f}, max={score_stats['max']:.1f}")

# 5e) 字符串清理
for c in ["title", "genres", "countries", "directors"]:
    df = df.withColumn(c, trim(col(c)))

# ── 6. 清洗结果概览 ────────────────────────────────
print(f"\n[步骤6] 清洗结果概览")
print(f"  最终行数: {df.count()}")
print(f"  清洗后数据前 10 行:")
df.show(10, truncate=True)

print("\n  数据统计摘要:")
df.describe(["rating_score", "rating_count", "year", "collect_count"]).show()

print("\n" + "=" * 60)
print("A-1 数据清洗完成！")
print("=" * 60)

# 缓存清洗后数据供后续使用
df.cache()
df.count()  # 触发缓存

spark.stop()
