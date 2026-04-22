FROM mcr.microsoft.com/playwright/python:v1.57.0-jammy

WORKDIR /app

# Keep Python logs unbuffered for easier docker logs tailing.
ENV PYTHONUNBUFFERED=1

COPY requirements.txt ./
RUN python3 -m pip install --no-cache-dir -r requirements.txt

COPY . .

# Defaults can be overridden via docker run -e / compose environment.
ENV BATCH_SIZE=10 \
    MAX_MATCHES=10 \
    POLL_INTERVAL_SECONDS=3600 \
    CONTINUOUS_RUN=true \
    DRY_RUN=false \
    PUT_LOGO_VALUE_FIELD=requested_key

CMD ["python3", "sync_no_logo_batch.py"]
