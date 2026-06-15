# ============================================================
# 附录 B-1: PySpark WordCount 入门示例
# 用途: 验证 Spark on K8s 环境部署成功 (作业提交验证用)
# 数据源: OBS (通过 S3A 协议)
# ============================================================
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# 读取示例文本（OBS 路径由教师提供，替换 <BUCKET>）
lines = spark.sparkContext.textFile("s3a://<BUCKET>/sample.txt")

word_counts = (
    lines.flatMap(lambda line: line.split())
         .map(lambda word: (word, 1))
         .reduceByKey(lambda a, b: a + b)
         .sortBy(lambda x: x[1], ascending=False)
)

print("Top 10 words:", word_counts.take(10))
spark.stop()
