FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Pre-create logs directory
RUN mkdir -p /app/logs

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Environment variables
ENV PORT=8000
ENV SYMBOL=BTCUSDT
ENV POLL_INTERVAL_MS=1000

EXPOSE 8000

# Use gunicorn with uvicorn workers for production stability
CMD gunicorn -w 1 -k uvicorn.workers.UvicornWorker main:app --bind 0.0.0.0:$PORT
