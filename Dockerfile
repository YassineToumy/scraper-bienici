FROM python:3.12-slim

WORKDIR /app

# Install cron + curl
RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy scripts
COPY scraper.py cleaner.py storage.py ./
COPY entrypoint.sh /entrypoint.sh
COPY runner.sh ./

RUN chmod +x runner.sh /entrypoint.sh
RUN mkdir -p /app/logs

ENTRYPOINT ["/entrypoint.sh"]
