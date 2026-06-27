FROM python:3.13-slim

# ffmpeg/ffprobe are the whole media pipeline (remux only, no transcode).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir "fastapi>=0.115" "uvicorn[standard]>=0.34"

COPY app ./app
COPY web ./web

EXPOSE 5000
# Launched via main.py's __main__ so we can pass ws_ping_interval=None (disables
# the keepalive ping that races media writes and kills the connection).
CMD ["python", "-m", "app.main"]
