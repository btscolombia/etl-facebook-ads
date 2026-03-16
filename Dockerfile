FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# El observador HTTP corre siempre para que Uptime Kuma (u otros) puedan monitorear el estado.
# El pipeline se ejecuta con: docker exec <container> python facebook_ads_pipeline.py
# o desde Schedule Jobs en Dokploy.
EXPOSE 9180
CMD ["python", "observer.py", "--serve", "--port", "9180"]
