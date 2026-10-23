FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY transfer_engine ./transfer_engine
COPY control_plane ./control_plane
RUN python -m pip install --no-cache-dir . \
    && mkdir -p /data
EXPOSE 5050
VOLUME ["/data"]
CMD ["python", "-m", "transfer_engine.receiver", "--host", "0.0.0.0", "--port", "5050", "--output-dir", "/data"]
