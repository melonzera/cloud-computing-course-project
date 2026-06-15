# CI/CD 流水线配置指南

## 一、流水线架构

```
Git Push (main)
    │
    ▼
┌────────────────┐
│ Stage 0: 代码质量 │ ← 语法检查 + 依赖验证（CI 门禁）
└───────┬────────┘
        ▼
┌────────────────┐
│ Stage 1: 构建镜像 │ ← Docker Build → Push SWR（backend + frontend）
└───────┬────────┘
        ▼
┌────────────────┐
│ Stage 2: 部署到CCE│ ← kubectl set image + apply
└───────┬────────┘
        ▼
┌────────────────┐
│ Stage 3: 冒烟测试 │ ← /api/ping 验证 + Redis 连通性
└───────┬────────┘
        ▼
┌────────────────┐
│ Stage 4: GitOps  │ ← 镜像 Tag 回写 YAML → commit 回仓库
└────────────────┘
```

## 二、配置 GitHub Secrets

进入仓库 **Settings → Secrets and variables → Actions → New repository secret**，依次添加：

| Secret 名称 | 说明 | 获取方式 |
|---|---|---|
| `SWR_ORG` | SWR 组织名 | SWR 控制台 → 组织管理 |
| `HW_ACCESS_KEY` | 华为云 AK | IAM → 访问密钥 |
| `HW_SECRET_KEY` | 华为云 SK | IAM → 访问密钥 |
| `KUBE_CONFIG` | CCE 集群 kubeconfig（base64） | `cat ~/.kube/config \| base64 -w0` |

### 详细获取方式

### 2.1 获取 SWR_ORG
登录华为云 SWR 控制台 → 组织管理 → 复制组织名称（如 `my-org-2025xxx`）

### 2.2 获取华为云 AK/SK
1. 华为云控制台 → 统一身份认证服务 IAM
2. 左侧「我的凭证」→「访问密钥」
3. 创建访问密钥 → 下载 CSV 文件
4. CSV 中 `Access Key Id` = `HW_ACCESS_KEY`，`Secret Access Key` = `HW_SECRET_KEY`

### 2.3 获取 CCE Kubeconfig
**在能访问 CCE 集群的机器上执行：**
```bash
# 下载 kubeconfig 并 base64 编码
cat ~/.kube/config | base64 -w0
# 或者在 Windows PowerShell:
[Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes((Get-Content ~/.kube/config -Raw)))
```
将输出的长字符串粘贴到 `KUBE_CONFIG` secret 中。

> ⚠️ **安全提醒**：kubeconfig 包含集群凭证，务必用 Secret 存储，**不要硬编码**到 YAML 文件中。

## 三、触发流水线

### 方式1：Push 代码自动触发
```bash
git add .
git commit -m "feat: update backend logic"
git push origin main
```

### 方式2：手动触发
GitHub 仓库 → **Actions** → **CI/CD Pipeline** → **Run workflow**

## 四、验证流水线成功

### 4.1 检查 GitHub Actions
Actions 页面中，所有 Stage 应显示 ✅ Passed

### 4.2 检查 K8s 镜像 Tag 已更新
```bash
kubectl get deployment backend -o jsonpath='{.spec.template.spec.containers[0].image}'
kubectl get deployment frontend -o jsonpath='{.spec.template.spec.containers[0].image}'
```
输出示例：
```
swr.cn-north-4.myhuaweicloud.com/my-org/backend:a1b2c3d4
swr.cn-north-4.myhuaweicloud.com/my-org/frontend:a1b2c3d4
```
Tag `a1b2c3d4` = Git commit SHA 前8位,可追溯到具体代码提交。

### 4.3 检查 Pod 运行状态
```bash
kubectl get pods -o wide
```
所有 Pod STATUS 应为 `Running`，RESTARTS 为 `0`。

### 4.4 冒烟测试
```bash
ELB_IP=$(kubectl get svc backend-svc -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
curl http://$ELB_IP/api/ping
# 预期返回: {"status":"ok"}
```

## 五、镜像 Tag 策略

| Tag 类型 | 格式 | 用途 |
|---|---|---|
| `latest` | `backend:latest` | 开发环境快速迭代 |
| Git SHA | `backend:a1b2c3d4` | 生产部署 + 回滚（可追溯） |
| 手动 Tag | `backend:v1.2.0` | 正式版本发布 |

## 六、回滚操作

```bash
# 回滚到上一个版本
kubectl rollout undo deployment/backend
kubectl rollout undo deployment/frontend

# 回滚到指定版本
kubectl rollout undo deployment/backend --to-revision=2

# 查看部署历史
kubectl rollout history deployment/backend
```

## 七、注意事项

1. **华为云 ELB** 需要提前在 CCE 集群中安装 `lb-addon` 插件
2. **SWR 镜像权限**设为公开，否则 CCE 节点无法拉取镜像
3. **PVC** 需要 CCE 集群已安装 EVS CSI 插件（`csi-disk` StorageClass）
4. 首次部署需手动 apply 一次 `configmap.yaml` 和 `secret.yaml`，后续 CI 会自动更新
