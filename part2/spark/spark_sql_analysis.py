"""
云计算技术课程设计 — 方向A：Spark SQL 统计分析（A-2，进阶级 15分）
使用 Spark SQL 对清洗后数据进行多维度统计分析。
包括：分组聚合、窗口函数、子查询、多维交叉分析。
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, round as spark_round,
    row_number, rank, dense_rank, percent_rank,
    max as spark_max, min as spark_min, stddev,
    desc, asc, lit, when
)
from pyspark.sql.window import Window
import time

# ── 创建 Spark Session ─────────────────────────────
spark = SparkSession.builder \
    .appName("SparkSQLAnalysis") \
    .enableHiveSupport() \
    .getOrCreate()

# ── 配置数据路径 ──────────────────────────────────
INPUT_PATH = "s3a://<BUCKET>/cleaned_movies"          # ← 替换为实际 OBS 路径
LOCAL_PATH = "/opt/spark/work/douban_movies.csv"    # 容器内本地测试路径

print("=" * 60)
print("Spark SQL 统计分析")
print("=" * 60)

# ── 1. 加载数据 ────────────────────────────────────
print("\n[1] 加载清洗后数据...")
try:
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(INPUT_PATH)
except Exception as e:
    print(f"警告: 无法从 OBS 读取 ({e})")
    print("尝试从本地加载 douban_movies.csv ...")
    try:
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("encoding", "UTF-8") \
            .csv(LOCAL_PATH)
        print("✓ 从本地加载成功")
    except Exception as e2:
        print(f"本地也读取失败 ({e2})，生成示例数据进行演示...")
        # 豆瓣电影示例数据
        sample_data = [
            (1292052, "肖申克的救赎", "The Shawshank Redemption", 1994.0, 9.7, 1992071, "犯罪/剧情", "美国", "弗兰克·德拉邦特", 2866850, "希望让人自由"),
            (1291546, "霸王别姬", "霸王别姬", 1993.0, 9.6, 1486938, "剧情/爱情/同性", "中国大陆/中国香港", "陈凯歌", 2266871, "风华绝代"),
            (1292720, "阿甘正传", "Forrest Gump", 1994.0, 9.5, 1509559, "剧情/爱情", "美国", "罗伯特·泽米吉斯", 2435797, "人生就像巧克力"),
            (1295644, "这个杀手不太冷", "Léon", 1994.0, 9.4, 1703344, "剧情/动作/犯罪", "法国", "吕克·贝松", 2788701, "大叔与萝莉"),
            (1292063, "美丽人生", "La vita è bella", 1997.0, 9.5, 1111122, "剧情/喜剧/战争", "意大利", "罗伯托·贝尼尼", 1587560, "最美的谎言"),
            (1295124, "辛德勒的名单", "Schindler's List", 1993.0, 9.5, 803456, "剧情/历史/战争", "美国", "史蒂文·斯皮尔伯格", 1298743, "拯救一个人就是拯救全世界"),
            (3541415, "盗梦空间", "Inception", 2010.0, 9.3, 1512567, "剧情/科幻/悬疑", "美国/英国", "克里斯托弗·诺兰", 2389034, "梦境与现实"),
            (1291561, "千与千寻", "千と千尋の神隠し", 2001.0, 9.4, 1667890, "剧情/动画/奇幻", "日本", "宫崎骏", 2543678, "不忘初心"),
            (1292722, "泰坦尼克号", "Titanic", 1997.0, 9.4, 1467890, "剧情/爱情/灾难", "美国/墨西哥", "詹姆斯·卡梅隆", 2345671, "You jump, I jump"),
            (1292064, "楚门的世界", "The Truman Show", 1998.0, 9.3, 1178901, "剧情/科幻", "美国", "彼得·威尔", 1786543, "假如生活欺骗了你"),
            (1300267, "星际穿越", "Interstellar", 2014.0, 9.3, 1234567, "剧情/科幻/冒险", "美国/英国/加拿大", "克里斯托弗·诺兰", 1987654, "爱能穿越时空"),
            (1929463, "少年派的奇幻漂流", "Life of Pi", 2012.0, 9.1, 1056789, "剧情/奇幻/冒险", "美国/中国台湾/英国", "李安", 1675432, "信仰的力量"),
            (1291843, "黑客帝国", "The Matrix", 1999.0, 9.0, 567890, "动作/科幻", "美国", "沃卓斯基姐妹", 987654, "什么是真实"),
            (1292220, "飞越疯人院", "One Flew Over the Cuckoo's Nest", 1975.0, 9.1, 456789, "剧情", "美国", "米洛斯·福尔曼", 765432, "不自由毋宁死"),
            (1887956, "你的名字。", "君の名は。", 2016.0, 8.5, 987654, "剧情/爱情/动画", "日本", "新海诚", 1876543, "跨越时空的相遇"),
            (1293182, "十二怒汉", "12 Angry Men", 1957.0, 9.4, 345678, "剧情", "美国", "西德尼·吕美特", 567890, "合理怀疑的力量"),
            (2131459, "蝙蝠侠：黑暗骑士", "The Dark Knight", 2008.0, 9.2, 789012, "剧情/动作/科幻/犯罪", "美国/英国", "克里斯托弗·诺兰", 1456789, "黑暗中的正义"),
            (1302425, "喜剧之王", "喜剧之王", 1999.0, 8.7, 678901, "剧情/喜剧/爱情", "中国香港", "周星驰/李力持", 1234567, "我养你啊"),
            (2043546, "让子弹飞", "让子弹飞", 2010.0, 8.9, 1123456, "剧情/喜剧/动作", "中国大陆/中国香港", "姜文", 1789012, "让子弹飞一会儿"),
            (3742360, "寄生虫", "기생충", 2019.0, 8.8, 890123, "剧情", "韩国", "奉俊昊", 1567890, "贫富差距的隐喻"),
        ]
        columns = ["movie_id", "title", "original_title", "year", "rating_score",
                    "rating_count", "genres", "countries", "directors", "collect_count", "summary"]
        df = spark.createDataFrame(sample_data, columns)

# ── 注册临时视图 ────────────────────────────────────
df.createOrReplaceTempView("movies")
print(f"数据行数: {df.count()}, 列数: {len(df.columns)}")

# ── 2. 整体统计概览 ────────────────────────────────
print("\n" + "=" * 60)
print("[2] 整体统计概览")
print("=" * 60)

print("\n▶ 各类型电影数量 Top 10（genre 以 / 分隔，取第一个）:")
spark.sql("""
    SELECT SPLIT(genres, '/')[0] AS main_genre,
           COUNT(*) AS movie_count,
           ROUND(AVG(rating_score), 2) AS avg_rating,
           ROUND(SUM(rating_count)) AS total_ratings
    FROM movies
    WHERE genres != '未知' AND genres IS NOT NULL
    GROUP BY main_genre
    ORDER BY movie_count DESC
    LIMIT 10
""").show()

print("\n▶ 各国电影评分对比 Top 10:")
spark.sql("""
    SELECT SPLIT(countries, '/')[0] AS main_country,
           COUNT(*) AS movie_count,
           ROUND(AVG(rating_score), 2) AS avg_rating,
           ROUND(MAX(rating_score), 1) AS max_rating,
           ROUND(SUM(rating_count)) AS total_ratings
    FROM movies
    WHERE countries != '未知' AND countries IS NOT NULL
    GROUP BY main_country
    HAVING COUNT(*) >= 2
    ORDER BY avg_rating DESC
    LIMIT 10
""").show()

# ── 3. 时间维度分析 ────────────────────────────────
print("=" * 60)
print("[3] 时间维度分析")
print("=" * 60)

print("\n▶ 各年份高分电影数量（评分 ≥ 9.0）:")
spark.sql("""
    SELECT CAST(year AS INT) AS year,
           COUNT(*) AS high_rated_count,
           ROUND(AVG(rating_score), 2) AS avg_rating
    FROM movies
    WHERE year >= 1950 AND rating_score >= 9.0
    GROUP BY CAST(year AS INT)
    ORDER BY year DESC
""").show(30, truncate=False)

print("\n▶ 各年代电影平均评分趋势:")
spark.sql("""
    SELECT CONCAT(FLOOR(year / 10) * 10, 's') AS decade,
           COUNT(*) AS movie_count,
           ROUND(AVG(rating_score), 2) AS avg_rating,
           ROUND(AVG(rating_count)) AS avg_votes
    FROM movies
    WHERE year >= 1930 AND year < 2025
    GROUP BY FLOOR(year / 10) * 10
    ORDER BY decade
""").show(truncate=False)

# ── 4. 窗口函数分析 ────────────────────────────────
print("=" * 60)
print("[4] 窗口函数分析 — 各类别订单金额排名")
print("=" * 60)

print("\n▶ 各类型内电影评分排名 (Top 3):")
spark.sql("""
    SELECT * FROM (
        SELECT SPLIT(genres, '/')[0] AS main_genre,
               title, rating_score, rating_count,
               ROW_NUMBER() OVER (PARTITION BY SPLIT(genres, '/')[0] ORDER BY rating_score DESC) AS genre_rank,
               RANK() OVER (PARTITION BY SPLIT(genres, '/')[0] ORDER BY rating_score DESC) AS dense_rank
        FROM movies
        WHERE genres != '未知' AND rating_count > 1000
    ) t
    WHERE genre_rank <= 3
    ORDER BY main_genre, genre_rank
""").show(30, truncate=False)

print("\n▶ 全球电影评分 Top 10 (评分人数 > 10000):")
spark.sql("""
    SELECT title, countries, year,
           rating_score, rating_count,
           ROUND(PERCENT_RANK() OVER (ORDER BY rating_score DESC), 3) AS percentile
    FROM movies
    WHERE rating_count > 10000
    ORDER BY rating_score DESC
    LIMIT 10
""").show(truncate=False)

# ── 5. RFM 分析雏形 ────────────────────────────────
print("=" * 60)
print("[5] 客户/类别价值分析")
print("=" * 60)

print("\n▶ 导演作品平均评分分析 (作品数 ≥ 2):")
spark.sql("""
    SELECT SPLIT(directors, '/')[0] AS main_director,
           COUNT(*) AS work_count,
           ROUND(AVG(rating_score), 2) AS avg_rating,
           ROUND(MAX(rating_score), 1) AS best_rating,
           ROUND(SUM(rating_count)) AS total_votes
    FROM movies
    WHERE directors != '未知' AND directors IS NOT NULL
    GROUP BY main_director
    HAVING COUNT(*) >= 2
    ORDER BY avg_rating DESC
    LIMIT 10
""").show()

print("\n▶ 高评分人数 vs 高评分 交叉分析:")
spark.sql("""
    SELECT
        CASE WHEN rating_count >= 1000000 THEN '超热门(≥100万)'
             WHEN rating_count >= 500000 THEN '热门(≥50万)'
             WHEN rating_count >= 100000 THEN '中等(≥10万)'
             ELSE '冷门(<10万)' END AS popularity,
        CASE WHEN rating_score >= 9.0 THEN '神作(≥9.0)'
             WHEN rating_score >= 8.0 THEN '佳作(≥8.0)'
             WHEN rating_score >= 7.0 THEN '良好(≥7.0)'
             ELSE '一般(<7.0)' END AS quality,
        COUNT(*) AS count
    FROM movies
    GROUP BY 1, 2
    ORDER BY 1, 2
""").show(20, truncate=False)

# ── 6. 交叉分析 ────────────────────────────────────
print("=" * 60)
print("[6] 交叉分析 — 状态 × 类别")
print("=" * 60)

print("\n▶ 类型 × 国家 交叉统计 (Top 组合):")
spark.sql("""
    SELECT SPLIT(genres, '/')[0] AS genre,
           SPLIT(countries, '/')[0] AS country,
           COUNT(*) AS cnt,
           ROUND(AVG(rating_score), 2) AS avg_rating
    FROM movies
    WHERE genres != '未知' AND countries != '未知'
      AND genres IS NOT NULL AND countries IS NOT NULL
    GROUP BY 1, 2
    ORDER BY cnt DESC
    LIMIT 15
""").show(truncate=False)

print("\n▶ 高收藏电影 (collect_count > 平均值):")
avg_collect = df.select(avg("collect_count")).collect()[0][0]
spark.sql(f"""
    SELECT title, year, rating_score,
           collect_count, rating_count
    FROM movies
    WHERE collect_count > {avg_collect}
      AND rating_score >= 8.5
    ORDER BY collect_count DESC
    LIMIT 10
""").show(truncate=False)

# ── 7. 高级统计 ────────────────────────────────────
print("=" * 60)
print("[7] 高级统计 — 高价值订单分析")
print("=" * 60)

print("=" * 60)
print("[7] 高级统计 — 评分分布与相关性分析")
print("=" * 60)

# 计算整体平均评分
avg_score = df.select(avg("rating_score")).collect()[0][0]
avg_votes = df.select(avg("rating_count")).collect()[0][0]
print(f"整体平均评分: {avg_score:.2f}")
print(f"平均评分人数: {avg_votes:.0f}")

print(f"\n▶ 高评分低关注度好片 (评分 > {avg_score + 0.5} 但评分人数 < 平均值):")
spark.sql(f"""
    SELECT title, year, rating_score, rating_count, genres
    FROM movies
    WHERE rating_score > {avg_score + 0.5}
      AND rating_count < {avg_votes}
    ORDER BY rating_score DESC
    LIMIT 10
""").show(truncate=False)

# ── 8. 汇总报告 ────────────────────────────────────
print("\n" + "=" * 60)
print("统计分析报告摘要")
print("=" * 60)

total_movies = df.count()
total_votes = df.select(spark_sum("rating_count")).collect()[0][0]
total_collects = df.select(spark_sum("collect_count")).collect()[0][0]
top_genre = df.groupBy("genres").count().orderBy(desc("count")).first()
print(f"• 总电影数: {total_movies}")
print(f"• 总评分人数: {total_votes:,}")
print(f"• 总收藏数: {total_collects:,}")
print(f"• 平均评分: {avg_score:.2f}")
print(f"• 最多电影类型: {top_genre[0]} ({top_genre[1]}部)")
print(f"• 涵盖国家/地区数: {df.select('countries').distinct().count()}")

print("\n✓ Spark SQL 统计分析完成！")
spark.stop()
