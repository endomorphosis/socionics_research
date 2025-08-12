# Minimal production Dockerfile
FROM python:3.11-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app

# System dependencies (if faiss-cpu needs libgomp etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential git curl && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
# Install dependencies first (no dev extras by default)
RUN pip install --no-cache-dir .

COPY src ./src

# Default env placeholders (override in runtime)
ENV SOCIONICS_EMBED_MODEL=sentence-transformers/all-MiniLM-L6-v2 \
    SOCIONICS_RATE_LIMIT_PER_MIN=15 \
    SOCIONICS_SEARCH_RATE_LIMIT_PER_MIN=30

CMD ["python", "-m", "bot.main"]
