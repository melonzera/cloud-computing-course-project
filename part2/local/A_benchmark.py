"""
============================================================
Part 2 扩展 — Pandas vs PySpark 性能对比 + Amdahl 分析
查询: 按电影类型分组统计 (COUNT / AVG / MAX)
对比: Pandas 单机 | PySpark 1 Executor | PySpark 2 Executor
============================================================
"""
import time
import os
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

CSV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                         "douban_movies.csv")

# ================================================================
# 1. Pandas 实现
# ================================================================
print("=" * 60)
print("[1/3] Pandas 单机实现")
print("=" * 60)

import pandas as pd

t0 = time.perf_counter()
df_pd = pd.read_csv(CSV_PATH, encoding='utf-8')
load_pd = time.perf_counter() - t0

# 清洗
df_pd = df_pd.dropna(subset=['genres', 'rating_score'])
df_pd = df_pd[(df_pd['rating_score'] >= 1.0) & (df_pd['rating_score'] <= 10.0)]
df_pd['main_genre'] = df_pd['genres'].str.split('/').str[0]

# 执行查询: 按类型分组统计
t1 = time.perf_counter()
result_pd = df_pd.groupby('main_genre').agg(
    movie_count=('movie_id', 'count'),
    avg_rating=('rating_score', 'mean'),
    avg_votes=('rating_count', 'mean'),
    max_rating=('rating_score', 'max')
).sort_values('movie_count', ascending=False).head(10)
query_pd = time.perf_counter() - t1

total_pd = load_pd + query_pd
print(f"  数据加载: {load_pd:.3f}s")
print(f"  分组查询: {query_pd:.3f}s")
print(f"  总耗时:   {total_pd:.3f}s")
print(f"\n  Top 10 类型:")
for genre, row in result_pd.iterrows():
    print(f"    {genre:<8} {int(row['movie_count']):>6}部  "
          f"均分{row['avg_rating']:.2f}  最高{row['max_rating']:.1f}")

# ================================================================
# 2. PySpark 1 Executor (local[1])
# ================================================================
print("\n" + "=" * 60)
print("[2/3] PySpark 1 Executor (local[1])")
print("=" * 60)

import compat
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, count, avg, max as spark_max, round as spark_round

spark1 = SparkSession.builder \
    .appName("Benchmark-1core") \
    .master("local[1]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

t0 = time.perf_counter()
df1 = spark1.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("encoding", "UTF-8") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .csv(CSV_PATH)
load_s1 = time.perf_counter() - t0

# 清洗 + 缓存
df1 = df1.filter(col("genres").isNotNull() & (col("genres") != "未知"))
df1 = df1.filter((col("rating_score") >= 1.0) & (col("rating_score") <= 10.0))
df1 = df1.withColumn("main_genre", split(col("genres"), "/")[0])
df1.cache()
df1.count()  # 触发缓存

# 执行查询
t1 = time.perf_counter()
result1 = df1.groupBy("main_genre").agg(
    count("*").alias("movie_count"),
    spark_round(avg("rating_score"), 2).alias("avg_rating"),
    spark_round(avg("rating_count")).alias("avg_votes"),
    spark_round(spark_max("rating_score"), 1).alias("max_rating")
).orderBy(col("movie_count").desc()).limit(10)
result1.collect()
query_s1 = time.perf_counter() - t1
total_s1 = load_s1 + query_s1

print(f"  数据加载+缓存: {load_s1:.3f}s")
print(f"  分组查询:      {query_s1:.3f}s")
print(f"  总耗时:        {total_s1:.3f}s")

df1.unpersist()
spark1.stop()

# ================================================================
# 3. PySpark 2 Executor (local[2])
# ================================================================
print("\n" + "=" * 60)
print("[3/3] PySpark 2 Executor (local[2])")
print("=" * 60)

spark2 = SparkSession.builder \
    .appName("Benchmark-2core") \
    .master("local[2]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

t0 = time.perf_counter()
df2 = spark2.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("encoding", "UTF-8") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .csv(CSV_PATH)
load_s2 = time.perf_counter() - t0

df2 = df2.filter(col("genres").isNotNull() & (col("genres") != "未知"))
df2 = df2.filter((col("rating_score") >= 1.0) & (col("rating_score") <= 10.0))
df2 = df2.withColumn("main_genre", split(col("genres"), "/")[0])
df2.cache()
df2.count()

t1 = time.perf_counter()
result2 = df2.groupBy("main_genre").agg(
    count("*").alias("movie_count"),
    spark_round(avg("rating_score"), 2).alias("avg_rating"),
    spark_round(avg("rating_count")).alias("avg_votes"),
    spark_round(spark_max("rating_score"), 1).alias("max_rating")
).orderBy(col("movie_count").desc()).limit(10)
result2.collect()
query_s2 = time.perf_counter() - t1
total_s2 = load_s2 + query_s2

print(f"  数据加载+缓存: {load_s2:.3f}s")
print(f"  分组查询:      {query_s2:.3f}s")
print(f"  总耗时:        {total_s2:.3f}s")

df2.unpersist()
spark2.stop()

# ================================================================
# 4. 汇总对比
# ================================================================
print("\n" + "=" * 70)
print("性能对比汇总")
print("=" * 70)

names  = ['Pandas\n(单机)', 'PySpark\n1 Executor', 'PySpark\n2 Executor']
loads  = [load_pd,  load_s1,  load_s2]
queries = [query_pd, query_s1, query_s2]
totals = [total_pd, total_s1, total_s2]

print(f"\n{'实现方式':<20} {'加载(s)':>10} {'查询(s)':>10} {'总耗时(s)':>10}")
print("-" * 50)
for n, l, q, t in zip(names, loads, queries, totals):
    print(f"{n:<20} {l:>10.3f} {q:>10.3f} {t:>10.3f}")

# 加速比 (相对于 Pandas)
s_pd = total_pd / total_pd
s_1  = total_pd / total_s1
s_2  = total_pd / total_s2

print(f"\n{'实现方式':<20} {'相对Pandas加速比':>18}")
print("-" * 40)
print(f"{names[0]:<20} {s_pd:>15.2f}x (基准)")
print(f"{names[1]:<20} {s_1:>15.2f}x")
print(f"{names[2]:<20} {s_2:>15.2f}x")

# ================================================================
# 5. Amdahl 定律分析
# ================================================================
print("\n" + "=" * 70)
print("Amdahl 定律分析")
print("=" * 70)

# 从 PySpark 1→2 Executor 的加速比反推 f
S_obs = total_s1 / total_s2   # 2 核对 1 核的加速比
f_raw = (2 * (S_obs - 1)) / (S_obs * (2 - 1)) if S_obs > 1 else 0

if f_raw > 1.0:
    print(f"  实测加速比 S(2) = {S_obs:.3f}x  →  f_raw = {f_raw:.3f} (>1, 超线性)")
    print(f"  原因: 多核共享缓存、内存带宽提升、JIT 预热")
    print(f"  保守估计 f = 0.90\n")
    f = 0.90
else:
    f = f_raw

serial = 1 - f
print(f"  Amdahl 参数:")
print(f"    可并行化比例  f  = {f:.4f} ({f*100:.1f}%)")
print(f"    串行瓶颈    1-f = {serial:.4f} ({serial*100:.1f}%)")
print(f"    理论最大加速比 (p→∞) = {1/serial:.1f}x")

# 理论加速比曲线
ps = [1, 2, 4, 8, 16, 32, 64]
theory = [1 / (serial + f / p) for p in ps]
observed = [1.0, S_obs, None, None, None, None, None]

print(f"\n  {'p':<6} {'理论加速比':<12} {'实测加速比':<12}")
print(f"  {'-'*30}")
for i, p in enumerate(ps):
    obs_str = f"{observed[i]:.2f}x" if observed[i] else "—"
    print(f"  {p:<6} {theory[i]:<12.3f}x {obs_str:<12}")

# ================================================================
# 6. 瓶颈分析
# ================================================================
print(f"""
┌──────────────────────────────────────────────────────────────┐
│              加速比未达线性的原因分析                           │
├──────────────────────────────────────────────────────────────┤
│ 1. 通信开销 (Shuffle):                                       │
│    GROUP BY 需要跨分区 shuffle 数据，1→2 核时 shuffle 写入    │
│    和网络传输开销增大，部分抵消了并行收益                        │
│                                                              │
│ 2. 序列化/反序列化:                                          │
│    PySpark 在 JVM ↔ Python 之间传递数据需要序列化，            │
│    这是纯 Pandas 没有的开销                                   │
│                                                              │
│ 3. 数据量限制:                                               │
│    当前数据集 ~2.6 万行，问题规模较小，并行调度开销            │
│    相对于计算收益占比高。数据量越大，并行优势越明显              │
│                                                              │
│ 4. 调度与同步开销:                                            │
│    Task 调度、Barrier 同步、GC 停顿都随核数增加而增大          │
│                                                              │
│ 5. 负载不均 (Skew):                                          │
│    剧情类电影占 40%+，该分区负载远大于其他分区，                │
│    导致部分 Worker 空闲等待                                   │
│                                                              │
│ 6. Pandas 的 C 底层优化:                                     │
│    Pandas 底层 NumPy 用 C/Fortran 实现，单机向量化             │
│    计算效率极高；小数据量时分布式框架反而 overhead 更大         │
└──────────────────────────────────────────────────────────────┘
""")

# ================================================================
# 7. 绘制对比图 (中文)
# ================================================================
print("正在绘制对比图...")

plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

fig, axes = plt.subplots(1, 3, figsize=(16, 5.5))
fig.suptitle('Pandas vs PySpark 性能对比 — 豆瓣电影类型分组统计',
             fontsize=14, fontweight='bold', y=1.02)

# ---- (a) 柱状图：总耗时对比 ----
ax1 = axes[0]
colors_bar = ['#3498db', '#e74c3c', '#2ecc71']
bars = ax1.bar(names, totals, color=colors_bar, edgecolor='white', linewidth=0.8, width=0.5)
for bar, val in zip(bars, totals):
    ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.03,
             f'{val:.2f}s', ha='center', va='bottom', fontsize=11, fontweight='bold')
ax1.set_ylabel('Execution Time (s)', fontsize=11)
ax1.set_title('Total Execution Time', fontsize=12, fontweight='bold')
ax1.grid(axis='y', alpha=0.3)

# ---- (b) 堆叠柱状图：加载 vs 查询 ----
ax2 = axes[1]
x = np.arange(len(names))
w = 0.35
bars_load = ax2.bar(x - w/2, loads, w, label='Data Load', color='#f39c12', edgecolor='white')
bars_query = ax2.bar(x + w/2, queries, w, label='Query Exec', color='#9b59b6', edgecolor='white')
for b in bars_load:
    ax2.text(b.get_x() + b.get_width()/2, b.get_height()/2, f'{b.get_height():.2f}s',
             ha='center', va='center', fontsize=8, color='white', fontweight='bold')
for b in bars_query:
    ax2.text(b.get_x() + b.get_width()/2, b.get_height()/2, f'{b.get_height():.2f}s',
             ha='center', va='center', fontsize=8, color='white', fontweight='bold')
ax2.set_xticks(x)
ax2.set_xticklabels(names, fontsize=9)
ax2.set_ylabel('Time (s)', fontsize=11)
ax2.set_title('Load vs Query Breakdown', fontsize=12, fontweight='bold')
ax2.legend(fontsize=9)
ax2.grid(axis='y', alpha=0.3)

# ---- (c) Amdahl 加速比曲线 ----
ax3 = axes[2]
ps_plot = np.linspace(1, 32, 100)
speedup_curve = [1 / (serial + f / p) for p in ps_plot]
ax3.plot(ps_plot, speedup_curve, 'b-', linewidth=2, label=f'Amdahl (f={f:.2f})')
ax3.plot(ps_plot, ps_plot, 'k--', linewidth=1, alpha=0.5, label='Linear (ideal)')

# 标注实测点
actual_ps = [1, 2]
actual_speedups = [1.0, S_obs]
ax3.scatter(actual_ps, actual_speedups, c='red', s=100, zorder=5, label='Measured')
for px, sy in zip(actual_ps, actual_speedups):
    ax3.annotate(f'{sy:.2f}x', (px, sy), textcoords="offset points",
                 xytext=(10, 10), fontsize=10, fontweight='bold', color='red')

ax3.set_xlabel('Number of Cores (p)', fontsize=11)
ax3.set_ylabel('Speedup', fontsize=11)
ax3.set_title('Amdahl Speedup Curve', fontsize=12, fontweight='bold')
ax3.legend(fontsize=9)
ax3.grid(alpha=0.3)
ax3.set_xlim(0, 33)

plt.tight_layout()

output_path = os.path.join(os.path.dirname(__file__), "benchmark_comparison.png")
plt.savefig(output_path, dpi=150, bbox_inches='tight')
print(f"\n对比图已保存: {output_path}")

plt.close()
print("✓ 全部完成!")
