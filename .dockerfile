# Fleet Monitor — long-running poller + dashboard.
# Works on Fly.io, Render, Railway, Koyeb, or any VPS with Docker.
FROM python:3.12-slim

# Unbuffered so logs stream to the platform's log viewer in real time.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    FLEET_HOSTED=1 \
    FLEET_STATE_DIR=/data

WORKDIR /app

# Dependencies first so code edits don't bust the layer cache.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code. api_client.py and auth.py must sit alongside this file.
COPY . .

# The dashboard is served by the app itself, from this directory.
# Persist caches/logs here — mount a volume at /data or history resets on
# every redeploy.
RUN mkdir -p /data
VOLUME ["/data"]

# The platform injects PORT; default matches local development.
ENV PORT=8080
EXPOSE 8080

# Run as a non-root user.
RUN useradd -m -u 10001 fleet && chown -R fleet:fleet /app /data
USER fleet

CMD ["python", "fleet_monitor_enhanced.py"]