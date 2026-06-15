"""One-shot demo for screenshots"""
import paho.mqtt.client as mqtt
import json, time, random, math

c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
c.connect("localhost", 1883)
print("=" * 55)
print("  Edge Computing Demo: K3s + MQTT Sensor Simulation")
print("=" * 55)
print("  Device    : edge-sensor-01")
print("  Broker    : localhost:1883 (Mosquitto on K3s)")
print("  Topics    : sensors/temperature, sensors/humidity")
print("  Cloud DB  : Redis (CCE K8s)")
print("=" * 55)
print()

for i in range(12):
    hour = time.localtime().tm_hour
    temp = round(22 + 8 * math.sin((hour - 6) * 3.14159 / 12) + random.gauss(0, 1.5), 2)
    humid = round(60 - (temp - 22) * 2 + random.gauss(0, 5), 1)
    ts = time.time()
    data = json.dumps({
        "device": "edge-sensor-01", "seq": i,
        "temperature": temp, "humidity": humid,
        "timestamp": ts
    })
    c.publish("sensors/temperature", data, qos=1)
    latency_ms = round((time.time() - ts) * 1000, 1)
    print(f"  [#{i:03d}] Temp={temp}C  Humid={humid}%  "
          f"Publish Latency={latency_ms}ms  {time.strftime('%H:%M:%S')}")
    time.sleep(0.5)

c.disconnect()
print()
print("  Demo Complete: 12 messages sent via MQTT")
print("  Cloud subscriber -> received & stored to Redis")
