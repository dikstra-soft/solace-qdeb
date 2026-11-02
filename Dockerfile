FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ common/
COPY consumer.py browser.py publisher.py xfer.py .env.example ./

ENV PYTHONUNBUFFERED=1

# Parked debug image for Kubernetes: PID 1 idles; exec in to run tools.
# Runs as root (simpler for kube troubleshooting / writable /app logs).
ENTRYPOINT ["sleep", "infinity"]
