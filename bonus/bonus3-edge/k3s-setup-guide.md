# 附加题3 (C-2): K3s + MQTT 边缘计算模拟

## 架构

```
┌─────────────────────────┐          MQTT          ┌─────────────────────────┐
│   边缘节点 (K3s)         │ ◄──────────────────► │   云端 (CCE K8s)         │
│                         │   Topic: sensors/#    │                         │
│  ┌─────────────────┐   │                        │  ┌─────────────────┐   │
│  │ Sensor Publisher │───►  sensors/temperature   │──│ MQTT Subscriber │   │
│  │ (Python)         │───►  sensors/humidity     │  │ → Redis Store   │   │
│  └─────────────────┘   │                        │  └─────────────────┘   │
│                         │                        │                         │
│  Mosquitto Broker       │                        │  Grafana Dashboard     │
│  (K3s Pod)              │                        │  (可视化云边延迟)       │
└─────────────────────────┘                        └─────────────────────────┘
```

## 环境搭建

### 1. 安装 K3s (轻量级 Kubernetes，适合边缘设备)

```bash
# 在边缘节点 (或本地 VM) 安装 K3s
curl -sfL https://get.k3s.io | sh -

# 验证
sudo k3s kubectl get nodes
sudo k3s kubectl get pods -A

# 获取 kubeconfig
sudo cat /etc/rancher/k3s/k3s.yaml
```

### 2. 在 K3s 上部署 Mosquitto MQTT Broker

```bash
# 创建 Mosquitto Deployment
cat <<EOF | sudo k3s kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mosquitto
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mosquitto
  template:
    metadata:
      labels:
        app: mosquitto
    spec:
      containers:
      - name: mosquitto
        image: eclipse-mosquitto:2
        ports:
        - containerPort: 1883
          name: mqtt
        - containerPort: 9001
          name: websocket
---
apiVersion: v1
kind: Service
metadata:
  name: mosquitto-svc
spec:
  type: NodePort
  selector:
    app: mosquitto
  ports:
  - port: 1883
    targetPort: 1883
    nodePort: 31883
EOF
```

### 3. 运行演示

```bash
# 在边缘节点 (K3s)
pip install paho-mqtt
python mqtt_sensor_publisher.py

# 在云端 (或本地模拟)
pip install paho-mqtt redis
python mqtt_cloud_subscriber.py
```

### 4. 验证 Redis 数据

```bash
# 在 CCE 集群中
kubectl exec deploy/redis -- redis-cli -a <password> KEYS "sensor:*"
kubectl exec deploy/redis -- redis-cli -a <password> GET "sensor:edge-sensor-xxx:temperature"
```

## MQTT 协议在弱网环境下的适用性分析

### 协议特点
- **轻量级**: 最小固定头部仅 2 字节，适合低带宽边缘网络
- **发布/订阅模式**: 解耦生产者与消费者，边缘设备无需知道云端地址
- **QoS 三级**: QoS 0 (最多一次) / QoS 1 (至少一次) / QoS 2 (恰好一次)
- **遗嘱机制**: 客户端异常断开时自动发布遗嘱消息，适合不可靠网络
- **持久会话**: 断线重连后恢复订阅和未消费消息

### 弱网环境下的优势
1. **Keep-Alive 心跳**: 默认 60s，可调小到 15s 适应高延迟网络
2. **消息队列**: Broker 为离线客户端缓存消息
3. **流量控制**: 相比 HTTP 轮询，MQTT 的推送模式减少 90%+ 不必要的网络请求

### 云边协同延迟挑战
1. **物理距离**: 边缘→云端 RTT 通常 10~200ms，跨地域可达 500ms+
2. **网络抖动**: 4G/5G/WiFi 切换导致延迟突增
3. **带宽受限**: 边缘网络上行带宽通常 < 10Mbps
4. **Broker 瓶颈**: 单 Mosquitto 实例约支持 10K 并发连接，大规模需集群化

### 优化策略
- 边缘端预处理：在 K3s 节点上做数据清洗/聚合后再上传
- QoS 1 替代 QoS 2：减少握手往返次数
- MQTT Bridge：多级 Broker 级联，边缘 Broker → 区域 Broker → 云端 Broker

## 截图清单
1. K3s 节点 `kubectl get nodes` (边缘节点)
2. Mosquitto Pod Running 状态
3. Sensor Publisher 连续输出截图
4. Cloud Subscriber 接收数据 + Redis 写入截图
5. Grafana 展示云边延迟趋势图 (可选，使用 Prometheus Pushgateway)
