FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ common/
COPY consumer.py browser.py publisher.py xfer.py .env.example ./

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/bin/bash"]
