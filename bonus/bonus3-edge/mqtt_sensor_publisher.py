"""
============================================================
附加题3 (C-2): 边缘计算模拟 — MQTT 传感器数据发布端
模拟场景: 边缘设备 (K3s 节点) 采集温度/湿度，通过 MQTT 发布到云端
依赖: pip install paho-mqtt
运行: python mqtt_sensor_publisher.py
============================================================
"""
import paho.mqtt.client as mqtt
import json
import time
import random
import math
import socket

# ── 配置 ──────────────────────────────────────────
MQTT_BROKER = "localhost"      # MQTT Broker 地址 (边缘端)
MQTT_PORT = 1883
TOPIC_TEMP = "sensors/temperature"
TOPIC_HUMID = "sensors/humidity"
TOPIC_STATUS = "sensors/status"
DEVICE_ID = f"edge-sensor-{socket.gethostname()[:8]}"
PUBLISH_INTERVAL = 2  # 秒

# ── 模拟传感器数据生成 ────────────────────────────
def generate_sensor_data():
    """生成模拟温湿度数据，带昼夜变化和随机噪声"""
    hour = time.localtime().tm_hour
    # 基础温度随昼夜变化（模拟真实场景）
    base_temp = 22 + 8 * math.sin((hour - 6) * math.pi / 12)
    temperature = base_temp + random.gauss(0, 1.5)
    # 湿度与温度负相关
    humidity = 60 - (temperature - 22) * 2 + random.gauss(0, 5)
    humidity = max(20, min(95, humidity))

    return {
        "device_id": DEVICE_ID,
        "timestamp": time.time(),
        "temperature": round(temperature, 2),
        "humidity": round(humidity, 2),
        "unit_temp": "celsius",
        "unit_humid": "percent"
    }

# ── MQTT 回调 ────────────────────────────────────
def on_connect(client, userdata, flags, reason_code, properties=None):
    print(f"[MQTT] Connected to broker, rc={reason_code}")

def on_publish(client, userdata, mid):
    pass  # QoS 0, no callback needed

def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=DEVICE_ID)
    client.on_connect = on_connect

    print(f"=== Edge Sensor Publisher ===")
    print(f"Device: {DEVICE_ID}")
    print(f"Broker: {MQTT_BROKER}:{MQTT_PORT}")
    print(f"Interval: {PUBLISH_INTERVAL}s")
    print(f"Topics: {TOPIC_TEMP}, {TOPIC_HUMID}, {TOPIC_STATUS}")
    print(f"{'='*50}")

    # 连接 MQTT Broker (弱网环境模拟：重连机制)
    connected = False
    retry_count = 0
    while not connected:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=30)
            connected = True
        except ConnectionRefusedError:
            retry_count += 1
            print(f"[MQTT] Connection refused, retry {retry_count}/10...")
            time.sleep(2)
            if retry_count >= 10:
                print("[MQTT] Max retries exceeded. Running in offline mode.")
                connected = True  # 离线模式继续运行

    # 发布状态上线消息
    status_msg = json.dumps({"device": DEVICE_ID, "status": "online"})
    client.publish(TOPIC_STATUS, status_msg, qos=1)
    print(f"[PUB] {TOPIC_STATUS}: {status_msg}")

    # ── 主循环：定时发布传感器数据 ──────────────────
    seq = 0
    try:
        while True:
            data = generate_sensor_data()
            data["seq"] = seq

            # 发布温度
            temp_payload = json.dumps(data)
            client.publish(TOPIC_TEMP, temp_payload, qos=1)

            # 发布湿度 (复用同一个 data，也可独立)
            humid_payload = json.dumps(data)
            client.publish(TOPIC_HUMID, humid_payload, qos=1)

            print(f"[PUB #{seq:04d}] Temp: {data['temperature']}C, "
                  f"Humidity: {data['humidity']}%, "
                  f"Time: {time.strftime('%H:%M:%S')}")

            seq += 1
            time.sleep(PUBLISH_INTERVAL)

    except KeyboardInterrupt:
        print("\n[MQTT] Shutting down...")
        offline_msg = json.dumps({"device": DEVICE_ID, "status": "offline"})
        client.publish(TOPIC_STATUS, offline_msg, qos=1)
        client.disconnect()
        print("[MQTT] Disconnected. Bye.")

if __name__ == "__main__":
    main()
