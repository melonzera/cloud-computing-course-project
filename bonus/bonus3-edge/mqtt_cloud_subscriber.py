"""
============================================================
附加题3 (C-2): 云端 MQTT 订阅者 — 接收边缘数据 → 存入 Redis
模拟场景: 云端 K8s 订阅 MQTT Topic，将传感器数据写入 Redis
依赖: pip install paho-mqtt redis
运行: python mqtt_cloud_subscriber.py
============================================================
"""
import paho.mqtt.client as mqtt
import json
import time
import redis
import os
from collections import deque

# ── 配置 ──────────────────────────────────────────
MQTT_BROKER = os.environ.get("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")

# 订阅的 Topic
TOPICS = [
    ("sensors/temperature", 1),
    ("sensors/humidity", 1),
    ("sensors/status", 1),
]

# 内存缓冲区 (最近 100 条记录)
buffer_size = 100
latest_readings = deque(maxlen=buffer_size)

# ── Redis 连接 ────────────────────────────────────
try:
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD or None,
        decode_responses=True,
    )
    r.ping()
    print(f"[Redis] Connected: {REDIS_HOST}:{REDIS_PORT}")
except Exception as e:
    print(f"[Redis] Connection failed: {e}")
    r = None

# ── 云边延迟统计 ──────────────────────────────────
latencies = deque(maxlen=500)

# ── MQTT 回调 ────────────────────────────────────
def on_connect(client, userdata, flags, reason_code, properties=None):
    print(f"[MQTT] Connected to broker at {MQTT_BROKER}:{MQTT_PORT}, rc={reason_code}")
    for topic, qos in TOPICS:
        client.subscribe(topic, qos=qos)
        print(f"[MQTT] Subscribed: {topic} (qos={qos})")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        device_id = payload.get("device_id", "unknown")
        timestamp = payload.get("timestamp", 0)
        temperature = payload.get("temperature")
        humidity = payload.get("humidity")

        # ── 计算云边延迟 ──────────────────────────────
        now = time.time()
        cloud_edge_latency = now - timestamp
        latencies.append(cloud_edge_latency)

        # ── 写入 Redis ────────────────────────────────
        if r:
            key_prefix = f"sensor:{device_id}"
            if temperature is not None:
                r.set(f"{key_prefix}:temperature", temperature)
                r.zadd(f"{key_prefix}:history:temp", {json.dumps(payload): timestamp})
            if humidity is not None:
                r.set(f"{key_prefix}:humidity", humidity)
            # 设置 TTL: 1 小时过期
            r.expire(f"{key_prefix}:temperature", 3600)
            r.expire(f"{key_prefix}:humidity", 3600)

        # ── 内存缓冲 ──────────────────────────────────
        latest_readings.append({
            "device": device_id,
            "topic": msg.topic,
            "temp": temperature,
            "humid": humidity,
            "latency_ms": round(cloud_edge_latency * 1000, 1),
        })

        # ── 打印 ──────────────────────────────────────
        if temperature is not None:
            avg_lat = (sum(latencies) / len(latencies) * 1000) if latencies else 0
            print(f"[SUB] {device_id} | Temp: {temperature}C | "
                  f"Edge→Cloud: {cloud_edge_latency*1000:.0f}ms | "
                  f"Avg Latency: {avg_lat:.0f}ms | "
                  f"Buffer: {len(latest_readings)}/{buffer_size}")

    except json.JSONDecodeError:
        pass  # 忽略非 JSON 消息
    except Exception as e:
        print(f"[ERROR] {e}")

def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="cloud-subscriber-001")
    client.on_connect = on_connect
    client.on_message = on_message

    print(f"=== Cloud MQTT Subscriber ===")
    print(f"Broker: {MQTT_BROKER}:{MQTT_PORT}")
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"{'='*50}")

    # 弱网环境：自动重连
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        client.loop_forever()
    except ConnectionRefusedError:
        print(f"[MQTT] Cannot connect to {MQTT_BROKER}:{MQTT_PORT}")
        print("[MQTT] Ensure MQTT Broker (e.g., Mosquitto) is running")
    except KeyboardInterrupt:
        print("\n[Cloud] Shutting down subscriber...")
        client.disconnect()

        # 打印最终统计
        if latencies:
            avg_lat = sum(latencies) / len(latencies) * 1000
            max_lat = max(latencies) * 1000
            min_lat = min(latencies) * 1000
            print(f"\n=== Cloud-Edge Latency Summary ===")
            print(f"  Samples: {len(latencies)}")
            print(f"  Average: {avg_lat:.1f}ms")
            print(f"  Max: {max_lat:.1f}ms")
            print(f"  Min: {min_lat:.1f}ms")

if __name__ == "__main__":
    main()
