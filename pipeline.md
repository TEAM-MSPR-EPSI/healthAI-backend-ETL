# Documentation — Pipeline CI/CD

## Vue d'ensemble

Cette pipeline GitHub Actions automatise la validation du code à chaque push et pull request. Elle est composée de trois jobs indépendants : `build`, `linter` et `test`, exécutés sur des runners `ubuntu-latest`.

```
push / pull_request (dev, main)
        │
        ▼
     ┌───────┐
     │ build │
     └───┬───┘
         │
   ┌─────┴─────┐
   ▼           ▼
┌────────┐  ┌──────┐
│ linter │  │ test │
└────────┘  └──────┘
```

`linter` et `test` dépendent tous les deux de `build` (`needs: build`) et s'exécutent en parallèle une fois que `build` a réussi.

## Déclencheurs (`on`)

```yaml
on:
  push:
    branches: ["**"]
  pull_request:
    branches: [dev, main]
```

La pipeline se déclenche sur :
- tout push, quelle que soit la branche
- toute pull request ciblant `dev` ou `main`

À noter : un push sur une branche suivi de l'ouverture d'une PR vers `dev`/`main` déclenche la pipeline deux fois pour le même commit (une fois via `push`, une fois via `pull_request`).

## Job : `build`

**Rôle** : vérifier que les dépendances Python du projet s'installent correctement.

| Étape | Description |
|---|---|
| `actions/checkout@v4` | Récupère le code du repo |
| `actions/setup-python@v5` | Installe Python 3.12, avec cache `pip` activé |
| Install dependencies | `pip install -r requirements.txt` |

Ce job ne partage pas son environnement avec les jobs suivants (chaque job tourne sur une machine vierge) ; il sert uniquement de garde-fou avant de lancer le lint et les tests.

## Job : `linter`

**Rôle** : vérifier la conformité du code aux règles de style PEP8 via `flake8`.

| Étape | Description |
|---|---|
| `actions/checkout@v4` | Récupère le code du repo |
| `actions/setup-python@v5` | Installe Python 3.12, avec cache `pip` activé |
| Install flake8 | `pip install flake8` |
| Run flake8 | `flake8 . --max-line-length=200 --exclude=.git,__pycache__,.venv` |

`--max-line-length=200` assouplit la limite par défaut de PEP8 (79 caractères). Les dossiers `.git`, `__pycache__` et `.venv` sont exclus de l'analyse.

## Job : `test`

**Rôle** : exécuter la suite de tests `pytest` à l'intérieur du conteneur Docker réel de l'application (`etl_backend`), avec une base de données PostgreSQL de test.

### Fichier `docker-compose.test.yml`

Un fichier compose dédié à la CI, séparé du `docker-compose.yml` du repo parent, situé à la racine du repo `healthAI-backend-ETL` :

```yaml
services:
  database:
    image: postgres:15-alpine
    container_name: postgres_db
    environment:
      POSTGRES_DB: testdb
      POSTGRES_USER: testuser
      POSTGRES_PASSWORD: testpassword
    networks:
      - app_network
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U testuser -d testdb"]
      interval: 10s
      timeout: 5s
      retries: 5

  etl_backend:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: etl_backend
    env_file:
      - .env
    environment:
      DATABASE_URL: postgresql://testuser:testpassword@database:5432/testdb
    volumes:
      - .:/app
    depends_on:
      database:
        condition: service_healthy
    networks:
      - app_network

networks:
  app_network:
```

Points clés de cette configuration :

- Les identifiants de la base de données (`testuser` / `testpassword` / `testdb`) sont écrits en dur. Cette base est éphémère, vide et détruite à chaque run, donc ces valeurs n'ont aucune portée de sécurité réelle.
- La variable `DATABASE_URL` est définie directement dans le bloc `environment` du service `etl_backend`, ce qui **écrase** toute valeur équivalente présente dans le `.env` — l'application se connecte donc systématiquement à la base de test, indépendamment du contenu du secret.
- Pas de volume de persistance sur `database` : chaque run démarre avec une base vierge.

### Étapes du job

| Étape | Description |
|---|---|
| `actions/checkout@v4` | Récupère le code du repo `healthAI-backend-ETL` |
| Create .env file | Génère le fichier `.env` à partir du secret GitHub `ETL_ENV_FILE` |
| Start containers (wait until healthy) | `docker compose -f docker-compose.test.yml up -d --build --wait` |
| Wait a bit for app startup | `sleep 5`, marge de sécurité pour laisser l'application terminer son initialisation |
| Run tests | `docker exec etl_backend pytest tests.py` |
| Show logs on failure | `docker compose -f docker-compose.test.yml logs` (uniquement si une étape précédente échoue) |
| Stop containers | `docker compose -f docker-compose.test.yml down` (toujours exécutée, via `if: always()`) |

```yaml
test:
  name: Tests
  runs-on: ubuntu-latest
  needs: build
  steps:
    - uses: actions/checkout@v4

    - name: Create .env file
      run: echo "${{ secrets.ETL_ENV_FILE }}" > .env

    - name: Start containers (wait until healthy)
      run: docker compose -f docker-compose.test.yml up -d --build --wait

    - name: Wait a bit for app startup
      run: sleep 5

    - name: Run tests
      run: docker exec etl_backend pytest tests.py

    - name: Show logs on failure
      if: failure()
      run: docker compose -f docker-compose.test.yml logs

    - name: Stop containers
      if: always()
      run: docker compose -f docker-compose.test.yml down
```

### Secret GitHub requis : `ETL_ENV_FILE`

Le `.env` de l'application est stocké comme un secret unique contenant l'intégralité du fichier (toutes les variables). Il doit être créé dans **Settings → Secrets and variables → Actions → Repository secrets** du repo `healthAI-backend-ETL`.

Ce secret couvre les variables d'environnement de l'application autres que la connexion à la base de données (qui est forcée via `DATABASE_URL` dans le compose).

## Architecture multi-repos (submodules)

Le projet est structuré en plusieurs repos Git distincts, chacun correspondant à un service (`healthAI-backend-ETL`, `healthAI-database`...), référencés comme submodules depuis un repo parent contenant le `docker-compose.yml` global de développement.

Le workflow CI documenté ici vit **dans le repo `healthAI-backend-ETL`** et se déclenche sur ses propres pushs/PRs. Il ne dépend donc pas du repo parent ni des autres submodules — c'est pour cette raison qu'un `docker-compose.test.yml` autonome (avec uniquement les services `database` et `etl_backend`) a été créé spécifiquement pour la CI, indépendamment du `docker-compose.yml` de développement complet.
