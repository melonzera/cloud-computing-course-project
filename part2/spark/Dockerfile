# ============================================================
# 附录 B-1 方向A: PySpark 镜像
# 教师提供的 SWR PySpark 镜像；作业脚本 copy 到容器内
# ============================================================
FROM apache/spark-py:v3.4.0

USER root

# 复制作业脚本
COPY wordcount.py /opt/spark/work/wordcount.py
COPY data_cleaning.py /opt/spark/work/data_cleaning.py
COPY spark_sql_analysis.py /opt/spark/work/spark_sql_analysis.py
COPY performance_comparison.py /opt/spark/work/performance_comparison.py
COPY douban_movies.csv /opt/spark/work/douban_movies.csv

WORKDIR /opt/spark/work
USER 185
