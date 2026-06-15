# 云计算课程设计 · 离线资源包使用说明

## 文件清单

```
离线包/
├── spark/
│   ├── spark-operator-2.5.0.tar   （770 MB）Spark Operator 控制器镜像
│   ├── pyspark-v9.tar             （1.1 GB）PySpark 作业运行时镜像
│   └── spark-operator/            （1.9 MB）Spark Operator Helm Chart（已解压，可直接 helm install）
├── mpi/
│   ├── mpi4py-latest.tar          （335 MB）mpi4py 运行时镜像（含 openssh-server）
│   └── mpi-operator.yaml          （568 KB）MPI Operator 完整部署清单（CRD + RBAC + Deployment）
└── monitoring/
    ├── monitoring-all.tar         （507 MB）Prometheus + Grafana 监控套件（含8个镜像）
    ├── kube-prometheus-stack-83.7.0.tgz  （854 KB）kube-prometheus-stack Helm Chart
    └── monitoring-values.yaml     自定义 Helm values（覆盖镜像地址等）
```

---

## 一、Docker 镜像包（.tar）

### 1. 加载镜像到本地 Docker

```bash
# Spark 相关
docker load -i spark/spark-operator-2.5.0.tar
docker load -i spark/pyspark-v9.tar

# MPI 相关
docker load -i mpi/mpi4py-latest.tar

# 监控套件
docker load -i monitoring/monitoring-all.tar
```

加载完成后用 `docker images` 确认所有镜像已出现。

### 2. 重新打 Tag（替换为你自己的 SWR 地址）

> 离线包中的镜像 Tag 来自原始制作环境。加载后需将镜像重新打 Tag，指向**你自己**的华为云 SWR 仓库，再推送供 CCE 集群使用。  
> 下面命令中 `<region>`、`<organization>` 替换为你实际的值，例如 `cn-east-3`、`my-org-2025xxxxxx`。

```bash
SWR="swr.<region>.myhuaweicloud.com/<organization>"

# Spark Operator
docker tag ghcr.io/kubeflow/spark-operator/controller:2.5.0 \
  ${SWR}/spark-operator:2.5.0

# PySpark
docker tag swr.cn-east-3.myhuaweicloud.com/cloud-course-2025212245/pyspark:v9 \
  ${SWR}/pyspark:v9

# mpi4py
docker tag swr.cn-east-3.myhuaweicloud.com/cloud-course-2025212245/mpi4py:latest \
  ${SWR}/mpi4py:latest

# 监控套件（8 个镜像）
OLD="swr.cn-east-3.myhuaweicloud.com/cloud-course-2025212245"
docker tag ${OLD}/grafana:12.4.3                    ${SWR}/grafana:12.4.3
docker tag ${OLD}/k8s-sidecar:2.6.0                 ${SWR}/k8s-sidecar:2.6.0
docker tag ${OLD}/prometheus:v3.11.2                ${SWR}/prometheus:v3.11.2
docker tag ${OLD}/alertmanager:v0.32.0              ${SWR}/alertmanager:v0.32.0
docker tag ${OLD}/prometheus-operator:v0.90.1       ${SWR}/prometheus-operator:v0.90.1
docker tag ${OLD}/kube-webhook-certgen:1.8.1        ${SWR}/kube-webhook-certgen:1.8.1
docker tag ${OLD}/prometheus-config-reloader:v0.90.1 ${SWR}/prometheus-config-reloader:v0.90.1
docker tag ${OLD}/node-exporter:v1.11.1             ${SWR}/node-exporter:v1.11.1
```

### 3. 登录 SWR 并推送

```bash
# 临时 Token：进入 SWR 控制台 → 右上角「登录指令」复制，24 小时有效
docker login -u <region>@<AK> -p <临时Token> swr.<region>.myhuaweicloud.com

docker push ${SWR}/spark-operator:2.5.0
docker push ${SWR}/pyspark:v9
docker push ${SWR}/mpi4py:latest
docker push ${SWR}/grafana:12.4.3
docker push ${SWR}/k8s-sidecar:2.6.0
docker push ${SWR}/prometheus:v3.11.2
docker push ${SWR}/alertmanager:v0.32.0
docker push ${SWR}/prometheus-operator:v0.90.1
docker push ${SWR}/kube-webhook-certgen:1.8.1
docker push ${SWR}/prometheus-config-reloader:v0.90.1
docker push ${SWR}/node-exporter:v1.11.1
```

推送完成后，在 SWR 控制台将镜像权限设为**公开**，CCE 集群节点无需额外认证即可拉取。

---

## 二、Helm Chart（离线安装）

### Spark Operator

Chart 已解压到 `course-design/spark/spark-operator/`，无需 `helm repo add`，直接用本地路径安装：

```bash
helm install spark-op spark/spark-operator/ \
  --set controller.image.repository=${SWR}/spark-operator \
  --set controller.image.tag=2.5.0 \
  --set webhook.image.repository=${SWR}/spark-operator \
  --set webhook.image.tag=2.5.0
```

### kube-prometheus-stack（监控套件）

使用压缩包直接安装，配合 `monitoring-values.yaml` 覆盖镜像地址：

```bash
helm upgrade --install monitoring monitoring/kube-prometheus-stack-83.7.0.tgz \
  -n monitoring --create-namespace \
  -f monitoring/monitoring-values.yaml
```

> `monitoring-values.yaml` 中所有 `<region>` 和 `<your-organization>` 已替换为占位符，安装前用文本编辑器全局替换为你自己的值即可（如 `cn-east-3`、`my-org-2025xxxxxx`）。

---

## 三、MPI Operator YAML

无需 Helm，直接 apply：

```bash
kubectl apply -f mpi/mpi-operator.yaml
```

该文件包含 MPI Operator 的全部资源（CRD、RBAC、Namespace、Deployment），一条命令完成部署。  
默认镜像为 `mpioperator/mpi-operator:master`，如节点无法访问 Docker Hub，需提前将镜像推送到 SWR 并修改文件中对应的 `image:` 字段。

---

## 四、各资源对应任务

| 资源 | 对应任务 |
|------|---------|
| `spark-operator-2.5.0.tar` + Helm Chart | 第二部分方向A：Spark Operator 安装 |
| `pyspark-v9.tar` | 第二部分方向A：WordCount SparkApplication |
| `mpi4py-latest.tar` + `mpi-operator.yaml` | 第二部分方向B：MPI on K8s |
| `monitoring-all.tar` + `kube-prometheus-stack-83.7.0.tgz` | 附加题1：Prometheus + Grafana 监控 |

---

## 五、常见问题

**Q：`docker load` 后 Tag 显示为空或 `<none>`？**  
A：用 `docker images -a` 找到对应 IMAGE ID，手动打 Tag：
```bash
docker tag <IMAGE_ID> ${SWR}/<镜像名>:<tag>
```

**Q：`docker load` 报 `no space left on device`？**  
A：进入 Docker Desktop → Settings → Resources → Disk image size 调大后重试。

**Q：推送到 SWR 报 `unauthorized`？**  
A：临时 Token 已过期，重新从 SWR 控制台获取登录指令后再推送。

**Q：`helm install` 报 `chart requires kubeVersion`？**  
A：检查集群版本是否满足 Chart 要求，通常 K8s 1.21+ 均可兼容。
