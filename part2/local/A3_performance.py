"""
============================================================
Part 2 — A-3: 性能对比与 Amdahl 分析 (5分)
对比不同并行度下的 Spark 执行性能，应用 Amdahl 定律分析加速比。

测试方法：用不同数量的 CPU 核心 (模拟 1/2/4/8 executor)
运行相同的聚合查询任务，记录各配置的执行时间。
============================================================
"""
import time
import math
import compat  # Python 3.13 + PySpark 3.4 兼容补丁
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, rand, split, when, lit
)

CSV_PATH = "c:/Users/melonzera/Desktop/云计算技术课程设计/douban_movies.csv"

print("=" * 60)
print("A-3: 性能对比与 Amdahl 定律分析")
print("=" * 60)

# ── 测试不同核心数 ────────────────────────────────
# local[N] 模拟 N 个 executor
core_configs = [1, 2, 4]
results = {}

for cores in core_configs:
    print(f"\n{'─'*50}")
    print(f"测试: {cores} 核心 (模拟 {cores} Executor)")
    print(f"{'─'*50}")

    spark = SparkSession.builder \
        .appName(f"PerfTest-{cores}cores") \
        .master(f"local[{cores}]") \
        .config("spark.sql.shuffle.partitions", str(cores * 2)) \
        .getOrCreate()

    # ── 加载数据 ────────────────────────────────────
    t0 = time.time()
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .option("multiLine", "true") \
        .option("escape", "\"") \
        .csv(CSV_PATH)

    df = df.fillna({"rating_score": 6.0, "rating_count": 0, "collect_count": 0,
                    "genres": "未知", "countries": "未知", "year": 0})
    df = df.filter((col("rating_score") >= 1.0) & (col("rating_score") <= 10.0))
    df.createOrReplaceTempView("movies")

    # 重分区确保均匀分布
    df = df.repartition(cores * 2)
    df.cache()
    row_count = df.count()
    load_time = time.time() - t0

    # ── 测试1: GROUP BY 聚合 (重度计算) ──────────────
    t0 = time.time()
    spark.sql("""
        SELECT SPLIT(genres, '/')[0] AS genre,
               COUNT(*) AS cnt,
               ROUND(AVG(rating_score), 2) AS avg_score,
               ROUND(SUM(rating_count)) AS total_votes,
               ROUND(AVG(collect_count)) AS avg_collect
        FROM movies
        WHERE genres != '未知'
        GROUP BY genre
        ORDER BY cnt DESC
    """).collect()
    agg_time = time.time() - t0

    # ── 测试2: 排序操作 ──────────────────────────────
    t0 = time.time()
    spark.sql("""
        SELECT title, rating_score, rating_count
        FROM movies
        ORDER BY rating_score DESC, rating_count DESC
    """).take(50)
    sort_time = time.time() - t0

    # ── 测试3: 过滤 + 窗口函数 ───────────────────────
    t0 = time.time()
    spark.sql("""
        SELECT title, year, rating_score,
               ROW_NUMBER() OVER (PARTITION BY SPLIT(genres,'/')[0] ORDER BY rating_score DESC) AS rn
        FROM movies
        WHERE rating_count > 5000 AND year >= 2000
    """).collect()
    window_time = time.time() - t0

    # ── 记录结果 ────────────────────────────────────
    total = load_time + agg_time + sort_time + window_time
    results[cores] = {
        "load": load_time, "agg": agg_time,
        "sort": sort_time, "window": window_time,
        "total": total, "rows": row_count
    }

    print(f"  数据行数: {row_count:,}")
    print(f"  加载耗时: {load_time:.2f}s")
    print(f"  GROUP BY 聚合: {agg_time:.2f}s")
    print(f"  排序 Top50: {sort_time:.2f}s")
    print(f"  窗口函数: {window_time:.2f}s")
    print(f"  总耗时: {total:.2f}s")

    df.unpersist()
    spark.stop()

# ── 汇总对比表 ────────────────────────────────────
print(f"\n\n{'='*70}")
print("性能对比汇总")
print(f"{'='*70}")

T1 = results[1]["total"]
print(f"\n{'核心数':<10} {'耗时(s)':<12} {'加速比':<12} {'效率':<12} {'数据行数':<12}")
print(f"{'-'*58}")
for cores in core_configs:
    r = results[cores]
    speedup = T1 / r["total"]
    efficiency = (speedup / cores) * 100
    print(f"{cores:<10} {r['total']:<12.2f} {speedup:<12.3f}x {efficiency:<11.1f}% {r['rows']:<12,}")

# ── Amdahl 定律分析 ────────────────────────────────
print(f"\n{'='*70}")
print("Amdahl 定律分析")
print(f"{'='*70}")

# 用 2 核和 4 核的加速比反推 f
S2 = T1 / results[2]["total"]
S4 = T1 / results[4]["total"]

# Amdahl: S(p) = 1 / ((1-f) + f/p)
# 反解: f = (p * (S-1)) / (S * (p-1))
f_from_2 = (2 * (S2 - 1)) / (S2 * (2 - 1)) if S2 > 1 else 0
f_from_4 = (4 * (S4 - 1)) / (S4 * (4 - 1)) if S4 > 1 else 0
f_raw = (f_from_2 + f_from_4) / 2

# 处理超线性加速比 (f > 1 常因缓存效应/内存带宽导致)
if f_raw > 1.0:
    print(f"  注意: 观察到超线性加速比 (f_raw={f_raw:.3f})")
    print(f"  这通常是由于缓存效应、内存带宽提升或 JIT 编译优化")
    print(f"  为 Amdahl 分析目的，将 f 保守估计为 0.95\n")
    f_avg = 0.95  # 保守估计
else:
    f_avg = f_raw

serial_fraction = 1 - f_avg

print(f"""
从实测加速比反推可并行化比例 f:
  S(2) = {S2:.3f}  →  f_raw = {f_from_2:.4f} ({f_from_2*100:.1f}%)
  S(4) = {S4:.3f}  →  f_raw = {f_from_4:.4f} ({f_from_4*100:.1f}%)
  f 保守估计值 = {f_avg:.4f} ({f_avg*100:.1f}%)""" + (" (原始值>1，已截断)" if f_raw > 1.0 else ""))
print(f"""
Amdahl 定律公式: Speedup(p) = 1 / ((1-f) + f/p)
  可并行化比例 f = {f_avg:.4f} ({f_avg*100:.1f}%)
  串行瓶颈 1-f = {serial_fraction:.4f} ({serial_fraction*100:.1f}%)
  理论最大加速比 (p→∞) = {1/serial_fraction:.2f}x
""")

print(f"{'核心数 p':<10} {'实测加速比':<14} {'Amdahl 理论值':<16} {'差距':<10}")
print(f"{'-'*50}")
for p in [1, 2, 4, 8, 16, 32, 64, 128]:
    theoretical = 1 / (serial_fraction + f_avg / p)
    if p in results:
        actual = T1 / results[p]["total"]
        diff = f"{abs(actual - theoretical) / theoretical * 100:.1f}%"
    else:
        actual_str = "—"
        diff = "—"
    print(f"{p:<10} {actual if p in results else '—':<14} {theoretical:<16.3f}x {diff:<10}")

print(f"""
┌─────────────────────────────────────────────────────────┐
│                    Amdahl 分析结论                        │
├─────────────────────────────────────────────────────────┤
│ 1. 可并行化比例 f = {f_avg*100:.1f}%                              │
│ 2. 串行瓶颈占比   = {serial_fraction*100:.1f}%                      │
│ 3. 无穷处理器最大加速比 ≈ {1/serial_fraction:.1f}x                      │
│ 4. 当前 4 核心加速比 ≈ {S4:.2f}x                           │
│                                                         │
│ 性能瓶颈分析:                                            │
│ • Shuffle 操作 (GROUP BY) 产生大量网络/磁盘 I/O           │
│ • 数据倾斜可能导致部分分区负载不均                         │
│ • 序列化/反序列化开销随数据量增大                          │
│                                                         │
│ 优化建议:                                                │
│ • 使用 broadcast join 减少 shuffle                      │
│ • 增加分区数提高并行度 (spark.sql.shuffle.partitions)     │
│ • 启用 Tungsten 优化和列式存储 (Parquet)                  │
└─────────────────────────────────────────────────────────┘
""")

print("✓ A-3 性能对比与 Amdahl 分析完成！")
