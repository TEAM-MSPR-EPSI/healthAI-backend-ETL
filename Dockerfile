# ── ÉTAPE 1 : Builder ──────────────────────────────────
FROM python:3.11-slim AS builder

LABEL org.opencontainers.image.title="healthAI-etl"
LABEL org.opencontainers.image.description="ETL pipeline pour le projet healthAI"
LABEL org.opencontainers.image.vendor="MSPR Team"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.source="https://github.com/TEAM-MSPR-EPSI/healthAI-backend-ETL"
LABEL org.opencontainers.image.version="1.0.0"
LABEL org.opencontainers.image.created="2026-06-16T12:00:00+02:00"

WORKDIR /app

COPY requirements.txt .

# Installation dans /install pour pouvoir copier proprement ensuite
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── ÉTAPE 2 : Runner (image finale légère) ─────────────
FROM python:3.11-slim AS runner

LABEL org.opencontainers.image.title="healthAI-etl"
LABEL org.opencontainers.image.description="ETL pipeline pour le projet healthAI"
LABEL org.opencontainers.image.vendor="MSPR Team"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.source="https://github.com/TEAM-MSPR-EPSI/healthAI-backend-ETL"
LABEL org.opencontainers.image.version="1.0.0"
LABEL org.opencontainers.image.created="2026-06-16T12:00:00+02:00"

WORKDIR /app

# Copie uniquement les packages installés (pip, caches... exclus)
COPY --from=builder /install /usr/local

# Copie uniquement les fichiers Python nécessaires
COPY etl.py etl_ingredient.py etl_exercise.py etl_load.py api.py ./

EXPOSE 8000

CMD ["python", "api.py"]
