FROM python:3.11-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
 && rm -rf /var/lib/apt/lists/*

# Install deps first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

# Copy app
COPY . .

# Environment defaults (override in compose or `docker run -e`)
ENV SAAS_QUEUE_BACKEND=thread \
    SAAS_DB_PATH=/app/data/runs.db \
    PYTHONUNBUFFERED=1

# Where SQLite lives
RUN mkdir -p /app/data

EXPOSE 8501

# Default command runs the web UI. Use `docker run ... python -m app.queue`
# to run the worker process instead.
CMD ["sh", "-c", "python -m streamlit run app/ui.py --server.port=${PORT:-8501} --server.address=0.0.0.0 --server.headless=true"]