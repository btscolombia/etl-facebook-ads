FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# El contenedor se mantiene vivo para que Dokploy pueda ejecutar jobs programados
# con: docker exec <container> python facebook_ads_pipeline.py
CMD ["sleep", "infinity"]
