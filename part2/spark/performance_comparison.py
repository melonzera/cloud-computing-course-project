"""
云计算技术课程设计 — 方向A：性能对比与 Amdahl 分析（A-3，挑战级 5分）
对比不同 Executor 数量下的 Spark 作业性能，并应用 Amdahl 定律分析。

验证步骤：
1. 修改 sparkapplication.yaml 中 executor.instances 分别为 1, 2, 4
2. 每次运行本脚本并记录执行时间
3. 根据输出结果完成 Amdahl 定律分析

本脚本在单一 executor 数量下运行，测量各部分耗时并推算 Amdahl 参数。
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, rand, monotonically_increasing_id
)
import time
import math

# ── 创建 Spark Session ─────────────────────────────
spark = SparkSession.builder \
    .appName("PerformanceComparison") \
    .getOrCreate()

sc = spark.sparkContext

print("=" * 60)
print("性能对比与 Amdahl 定律分析")
print("=" * 60)

# 获取当前 Executor 数量
executor_instances = int(sc.getConf().get("spark.executor.instances", "2"))
print(f"当前 Executor 数量: {executor_instances}")

# ── 生成大规模测试数据 ─────────────────────────────
print("\n[1] 生成测试数据 (1000万行 × 6列)...")
t0 = time.time()

NUM_ROWS = 10_000_000
num_partitions = executor_instances * 4

df = spark.range(NUM_ROWS) \
    .withColumn("category", (rand() * 10).cast("int")) \
    .withColumn("value", rand() * 1000) \
    .withColumn("quantity", (rand() * 20 + 1).cast("int")) \
    .withColumn("flag", (rand() * 2).cast("int")) \
    .repartition(num_partitions)

df.cache()
row_count = df.count()
data_gen_time = time.time() - t0
print(f"数据生成完成: {row_count:,} 行, "
      f"分区数: {df.rdd.getNumPartitions()}, "
      f"耗时: {data_gen_time:.2f}s")

# ── 2. 串行部分测量 ────────────────────────────────
print("\n[2] 测量串行部分（不可并行的开销）...")
serial_times = {}

# 2a) 数据读取/加载
t0 = time.time()
df.count()  # 触发 cache 加载
serial_times["数据加载"] = time.time() - t0

# 2b) 单分区操作（shuffle 前预处理）
t0 = time.time()
df.select("category", "value").count()
serial_times["简单投影"] = time.time() - t0

# ── 3. 并行部分测量 ────────────────────────────────
print("[3] 测量并行部分...")
parallel_times = {}

# 3a) GROUP BY 聚合（重度 shuffle）
t0 = time.time()
agg_result = df.groupBy("category").agg(
    count("*").alias("cnt"),
    spark_sum("value").alias("total_value"),
    avg("value").alias("avg_value"),
    spark_sum("quantity").alias("total_qty")
)
agg_result.collect()  # 触发计算
parallel_times["GROUP BY 聚合"] = time.time() - t0

# 3b) 排序操作
t0 = time.time()
df.orderBy("value", ascending=False).select("value").take(100)
parallel_times["排序 (Top 100)"] = time.time() - t0

# 3c) 过滤 + 聚合
t0 = time.time()
df.filter(col("value") > 500).groupBy("category").count().collect()
parallel_times["过滤+聚合"] = time.time() - t0

# ── 4. 结果汇总 ────────────────────────────────────
print("\n" + "=" * 60)
print("[4] 性能测量结果汇总")
print("=" * 60)

print(f"\n{'操作':<20} {'耗时(s)':>10}")
print("-" * 35)
total_serial = 0
for name, t in serial_times.items():
    print(f"{name:<20} {t:>10.3f}")
    total_serial += t

total_parallel = 0
for name, t in parallel_times.items():
    print(f"{name:<20} {t:>10.3f}")
    total_parallel += t

total_time = total_serial + total_parallel
print("-" * 35)
print(f"{'总耗时':<20} {total_time:>10.3f}")
print(f"\n串行部分占比: {total_serial/total_time*100:.1f}%")
print(f"并行部分占比: {total_parallel/total_time*100:.1f}%")

# ── 5. Amdahl 定律分析 ─────────────────────────────
print("\n" + "=" * 60)
print("[5] Amdahl 定律分析")
print("=" * 60)

# f: 可被并行化的比例 (并行部分占比)
f = total_parallel / total_time
serial_fraction = 1 - f  # 串行比例

print(f"""
Amdahl 定律公式: Speedup = 1 / ((1 - f) + f / p)

其中:
  f  = 可并行化比例 = {f:.4f} ({f*100:.1f}%)
  1-f = 串行部分比例 = {serial_fraction:.4f} ({serial_fraction*100:.1f}%)
  p  = 处理器数量 (Executor 数)

理论加速比预测:
""")

print(f"{'Executors':>10} {'理论加速比':>12} {'实际加速比(估)':>14} {'效率':>10}")
print("-" * 50)

# 基准时间 (当前 executor 数量下的总时间)
base_time = total_time
base_p = executor_instances

for p in [1, 2, 4, 8]:
    theoretical_speedup = 1 / (serial_fraction + f / p)

    # 估算实际执行时间 (基于 Amdahl)
    estimated_time = total_serial + total_parallel * base_p / p

    efficiency = (theoretical_speedup / p) * 100

    marker = " ← 当前" if p == executor_instances else ""
    print(f"{p:>10} {theoretical_speedup:>12.3f}x {estimated_time:>12.2f}s {efficiency:>8.1f}%{marker}")

# ── 6. 结论 ────────────────────────────────────────
print(f"""
┌─────────────────────────────────────────────────────┐
│                  Amdahl 分析结论                       │
├─────────────────────────────────────────────────────┤
│ 1. 可并行化比例 f = {f*100:.1f}%                           │
│ 2. 串行瓶颈占比 = {serial_fraction*100:.1f}%                  │
│ 3. 无穷多处理器下理论最大加速比 = {1/serial_fraction:.2f}x        │
│ 4. 当前 {executor_instances} Executor 加速比 ≈ {1/(serial_fraction+f/executor_instances):.2f}x    │
│                                                     │
│ 优化建议:                                            │
│ • 减少 shuffle 操作可提升 f 值                        │
│ • 增加数据分区数可提高并行度                           │
│ • 使用 broadcast join 替代 shuffle join              │
└─────────────────────────────────────────────────────┘
""")

print("✓ 性能对比与 Amdahl 分析完成！")
print(f"\n提示: 修改 sparkapplication.yaml 中 executor.instances")
print(f"      分别设为 1/2/4，重新运行本脚本，收集各组数据后")
print(f"      填入实验报告的 Amdahl 分析表格。")

# 清理
df.unpersist()
spark.stop()
