"""
云计算技术课程设计 — Flask 后端 API
提供 /api/ping 健康检查接口，连接 Redis 数据库。
"""
from flask import Flask, jsonify
import redis
import os

app = Flask(__name__)

# ── 从环境变量读取 Redis 配置 ──────────────────────
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")

# ── 连接 Redis ─────────────────────────────────────
r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD or None,
    decode_responses=True,
)


@app.route("/api/ping")
def ping():
    """健康检查接口，返回 {"status": "ok"}"""
    return jsonify({"status": "ok"})


@app.route("/api/redis/ping")
def redis_ping():
    """验证 Redis 连接状态"""
    try:
        r.ping()
        return jsonify({"redis": "connected"})
    except Exception as e:
        return jsonify({"redis": "error", "message": str(e)}), 500


@app.route("/api/redis/set")
def redis_set():
    """向 Redis 写入测试数据（用于验证持久化）"""
    r.set("testkey", "hello")
    return jsonify({"action": "SET testkey hello", "status": "ok"})


@app.route("/api/redis/get")
def redis_get():
    """从 Redis 读取测试数据"""
    value = r.get("testkey")
    return jsonify({"action": "GET testkey", "value": value})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
