"""
云计算技术课程设计 — 方向A：数据清洗（A-1，基础级 10分）
使用 PySpark 对原始数据集进行完整的数据清洗流程。
包括：缺失值处理、去重、类型转换、异常值检测。
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, isnan, isnull, count, mean, stddev,
    min as spark_min, max as spark_max, lit, trim
)
from pyspark.sql.types import IntegerType, DoubleType, StringType
import time

# ── 创建 Spark Session ─────────────────────────────
spark = SparkSession.builder \
    .appName("DataCleaning") \
    .getOrCreate()

# ── 配置数据路径 ──────────────────────────────────
# 生产环境：OBS 路径（部署到 CCE 后使用）
INPUT_PATH = "s3a://<BUCKET>/douban_movies.csv"    # ← 替换为实际 OBS 路径
OUTPUT_PATH = "s3a://<BUCKET>/cleaned_movies"       # ← 替换为实际 OBS 路径
# 本地测试：直接读取项目中的 CSV 文件
LOCAL_PATH = "/opt/spark/work/douban_movies.csv"     # 容器内路径

print("=" * 60)
print("数据清洗流程 — PySpark Data Cleaning")
print("=" * 60)

# ── 1. 读取原始数据 ────────────────────────────────
print("\n[步骤1] 读取原始数据...")
try:
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(INPUT_PATH)
except Exception as e:
    print(f"警告: 无法从 OBS 读取 ({e})")
    print("尝试从本地路径读取 douban_movies.csv ...")
    try:
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("encoding", "UTF-8") \
            .csv(LOCAL_PATH)
        print("✓ 从本地加载成功")
    except Exception as e2:
        print(f"本地也读取失败 ({e2})，生成示例数据进行演示...")
        # 生成带噪声的豆瓣电影示例数据
        sample_data = [
            (1292052, "肖申克的救赎", "The Shawshank Redemption", 1994.0, 9.7, 1992071, "犯罪/剧情", "美国", "弗兰克·德拉邦特", 2866850),
            (1291546, "霸王别姬", "霸王别姬", 1993.0, 9.6, 1486938, "剧情/爱情/同性", "中国大陆/中国香港", "陈凯歌", 2266871),
            (1292720, "阿甘正传", "Forrest Gump", 1994.0, 9.5, 1509559, "剧情/爱情", "美国", "罗伯特·泽米吉斯", 2435797),
            (1295644, "这个杀手不太冷", "Léon", 1994.0, 9.4, 1703344, "剧情/动作/犯罪", "法国", "吕克·贝松", 2788701),
            (1292052, "肖申克的救赎", "The Shawshank Redemption", 1994.0, 9.7, 1992071, "犯罪/剧情", "美国", "弗兰克·德拉邦特", 2866850),  # 重复行
            (9999999, "测试电影", None, None, -1.0, None, None, None, None, None),  # 异常/缺失数据
            (8888888, "低分电影", "Low Rating", 2020.0, 0.5, 10, "剧情", "美国", "未知导演", 5),
            (7777777, "高热电影", "Hot Movie", 2023.0, 9.9, 99999999, "喜剧", "中国", "知名导演", 99999999),
            (6666666, None, "Missing Title", 2022.0, 7.5, 5000, None, "英国", None, 1000),  # 缺失标题
            (5555555, "重复电影", "Duplicate", 2020.0, 6.0, 100, "剧情", "美国", "导演A", 200),
            (5555555, "重复电影", "Duplicate", 2020.0, 6.0, 100, "剧情", "美国", "导演A", 200),  # 完全重复
        ]
        columns = ["movie_id", "title", "original_title", "year", "rating_score",
                    "rating_count", "genres", "countries", "directors", "collect_count"]
        df = spark.createDataFrame(sample_data, columns)

print(f"原始数据行数: {df.count()}")
print(f"原始数据列数: {len(df.columns)}")
print(f"列名: {df.columns}")
print("\n原始数据前5行:")
df.show(5, truncate=False)

# ── 2. 缺失值分析 ──────────────────────────────────
print("\n[步骤2] 缺失值分析...")
missing_counts = df.select([
    count(when(col(c).isNull() | isnan(c), c)).alias(c)
    for c in df.columns
])
print("各列缺失值数量:")
missing_counts.show()

# 处理策略：
# - year 列：用 0 填充（表示未知年份）
# - rating_score 列：用均值填充
# - 字符串列（genres, countries, directors）：用"未知"填充
# - title/original_title：用"未知标题"填充
print("\n→ 填充缺失值")

# 计算 rating_score 的均值
if "rating_score" in df.columns:
    score_mean = df.select(mean(col("rating_score"))).collect()[0][0]
    df = df.fillna({"rating_score": score_mean if score_mean else 6.0})

# 数值列处理
df = df.fillna({"year": 0, "rating_count": 0, "collect_count": 0, "movie_id": 0})

# 字符串列填充
str_fill = {
    "title": "未知标题",
    "original_title": "Unknown Title",
    "genres": "未知",
    "countries": "未知",
    "directors": "未知",
}
df = df.fillna(str_fill)

# ── 3. 去重 ────────────────────────────────────────
print("\n[步骤3] 去重...")
before_dedup = df.count()
df = df.dropDuplicates()
after_dedup = df.count()
print(f"去重前: {before_dedup} 行 → 去重后: {after_dedup} 行")
print(f"删除重复行: {before_dedup - after_dedup}")

# ── 4. 数据类型转换 ────────────────────────────────
print("\n[步骤4] 数据类型转换...")
if "amount" in df.columns:
    df = df.withColumn("amount", col("amount").cast(DoubleType()))
if "quantity" in df.columns:
    df = df.withColumn("quantity", col("quantity").cast(IntegerType()))
print("Schema 检查:")
df.printSchema()

# ── 5. 异常值检测与处理 ────────────────────────────
print("\n[步骤5] 异常值检测与处理...")

# 评分范围检查 (豆瓣评分 0~10)
if "rating_score" in df.columns:
    before = df.count()
    df = df.filter((col("rating_score") >= 0) & (col("rating_score") <= 10))
    print(f"过滤异常评分 (<0 或 >10): {before} → {df.count()} (删除 {before - df.count()} 行)")

# 年份范围检查 (电影年份应在合理范围)
if "year" in df.columns:
    before = df.count()
    df = df.filter((col("year") >= 1888) | (col("year") == 0))  # 1888年第一部电影
    print(f"过滤异常年份 (<1888): {before} → {df.count()} (删除 {before - df.count()} 行)")

# 3σ 异常检测（评分人数）
if "rating_count" in df.columns:
    stats = df.select(
        mean("rating_count").alias("mean"),
        stddev("rating_count").alias("stddev")
    ).collect()[0]
    if stats["stddev"] and stats["stddev"] > 0:
        lower = max(0, stats["mean"] - 3 * stats["stddev"])
        upper = stats["mean"] + 3 * stats["stddev"]
        before = df.count()
        df = df.filter((col("rating_count") >= lower) & (col("rating_count") <= upper))
        print(f"rating_count 3σ 检测 (range: [{lower:.0f}, {upper:.0f}]): "
              f"{before} → {df.count()}")

# 评分分数统计
if "rating_score" in df.columns:
    stats = df.select(
        mean("rating_score").alias("mean"),
        stddev("rating_score").alias("stddev"),
        spark_min("rating_score").alias("min"),
        spark_max("rating_score").alias("max")
    ).collect()[0]
    print(f"评分统计: mean={stats['mean']:.2f}, stddev={stats['stddev']:.2f}, "
          f"min={stats['min']:.1f}, max={stats['max']:.1f}")

# 字符串列清理
str_cols = [f.name for f in df.schema.fields if f.dataType == StringType()]
for c in str_cols:
    if c in df.columns:
        df = df.withColumn(c, trim(col(c)))

# ── 6. 数据概览 ────────────────────────────────────
print("\n[步骤6] 清洗后数据概览...")
print(f"最终数据行数: {df.count()}")
print("清洗后数据前10行:")
df.show(10, truncate=False)

print("\n数据统计摘要:")
df.describe().show()

# ── 7. 输出清洗后数据 ──────────────────────────────
print(f"\n[步骤7] 保存清洗后数据 → {OUTPUT_PATH}")
try:
    df.write.mode("overwrite").option("header", "true").csv(OUTPUT_PATH)
    print("✓ 数据已保存到 OBS")
except Exception as e:
    print(f"注意: 无法写入 OBS ({e})")
    print("数据清洗流程演示完成（本地模式）")

print("\n" + "=" * 60)
print("数据清洗完成！")
print("=" * 60)
spark.stop()
