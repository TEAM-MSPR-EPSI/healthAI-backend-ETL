# ── ÉTAPE 1 : Builder ──────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

COPY requirements.txt .

# Installation dans /install pour pouvoir copier proprement ensuite
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── ÉTAPE 2 : Runner (image finale légère) ─────────────
FROM python:3.11-slim AS runner

WORKDIR /app

# Copie uniquement les packages installés (pip, caches... exclus)
COPY --from=builder /install /usr/local

# Copie uniquement les fichiers Python nécessaires
COPY etl.py etl_ingredient.py etl_exercise.py etl_load.py api.py ./

EXPOSE 8000

CMD ["python", "api.py"]
