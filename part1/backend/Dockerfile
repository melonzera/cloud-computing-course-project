# ============================================================
# 云计算技术课程设计 — Flask 后端 Dockerfile（多阶段构建）
# ============================================================

# ── Stage 1: 构建阶段 ─────────────────────────────────
FROM python:3.11-slim AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# ── Stage 2: 运行阶段 ─────────────────────────────────
FROM python:3.11-slim
WORKDIR /app
# 从构建阶段复制已安装的包
COPY --from=builder /root/.local /root/.local
# 复制应用代码
COPY app.py .
# 确保 pip 安装的包在 PATH 中
ENV PATH=/root/.local/bin:$PATH
EXPOSE 5000
CMD ["python", "app.py"]
