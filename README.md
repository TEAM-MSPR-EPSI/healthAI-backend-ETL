# healthAI-backend-ETL

API FastAPI pour orchestrer les pipelines ETL (Extract-Transform-Load) et charger les données en base de données PostgreSQL.

## Architecture

```
Client HTTP
    |
    |── POST /etl/extract-transform ──> API FastAPI ──> etl.py
    |                                                     |
    |                                                     |── Open Food Facts API
    |                                                     |
    |                                                     |── Wger API
    |                                                     |
    |                                                     |── ingredient_valid.csv
    |                                                     |── ingredient_invalid.csv
    |                                                     |── exercise_valid.csv
    |                                                     |── exercise_invalid.csv
    |
    |── GET /csv/{csv_name} ──> Serve CSV
    |
    |── POST /etl/load-to-db ──> etl_load.py ──> ingredient_valid.csv
                                                |── exercise_valid.csv
                                                |── PostgreSQL Database
```

## Endpoints

**1. Lancer le Pipeline ETL**

POST `/etl/extract-transform`

Exécute le fichier `etl.py` pour extraire et transformer les données.
- Récupère les données des APIs externes (Open Food Facts, Wger)
- Valide et nettoie les données
- Génère les fichiers CSV valides et rejetés

Réponse : `{ "status": "success", "message": "Pipeline ETL exécuté avec succès", "logs": "..." }`

**2. Charger les Données en BDD**

POST `/etl/load-to-db`

Exécute le fichier `etl_load.py` pour insérer les données valides en base de données.

Réponse : `{ "status": "success", "message": "Chargement en base de données exécuté avec succès", "logs": "..." }`

**4. Vérifier l'état de l'API**

GET `/health`

Retourne le statut de l'API.

**5. Lister les fichiers CSV**

GET `/csv`

Liste tous les fichiers CSV disponibles avec leur statut.

## Tester le Workflow

### Avec Swagger UI

1. Accéder à http://localhost:8000/docs
2. Tous les endpoints disponibles sont listés

**Étape 1 - Extract & Transform**
- Sélectionner `POST /etl/extract-transform`
- Cliquer sur "Try it out"
- Cliquer sur "Execute"
- Attendre la réponse `"status": "success"` (durée : quelques minutes)

**Étape 2 - Vérifier les fichiers CSV**
- Sélectionner `GET /csv`
- Cliquer sur "Try it out" puis "Execute"
- Vérifier que les 4 fichiers CSV existent avec `"exists": true`

**Étape 3 - Charger les données en base de données**
- Sélectionner `POST /etl/load-to-db`
- Cliquer sur "Try it out" puis "Execute"
- Attendre la réponse `"status": "success"`

### Avec Postman

1. Télécharger Postman depuis https://www.postman.com/downloads/
2. Créer une nouvelle collection "HealthAI-ETL"

**Étape 1 - Extract & Transform**
- Créer une nouvelle requête : POST
- URL : `http://localhost:8000/etl/extract-transform`
- Cliquer "Send"
- Attendre la réponse

**Étape 2 - Vérifier les fichiers CSV**
- Créer une nouvelle requête : GET
- URL : `http://localhost:8000/csv`
- Cliquer "Send"

**Étape 3 - Charger en base de données**
- Créer une nouvelle requête : POST
- URL : `http://localhost:8000/etl/load-to-db`
- Cliquer "Send"

### Test GET depuis le navigateur

Les endpoints GET peuvent être testés directement depuis la barre d'adresse du navigateur :

```
http://localhost:8000/health
http://localhost:8000/csv
```

Les endpoints POST nécessitent Swagger UI ou Postman.

## Démarrage

L'API se lance automatiquement lors du démarrage des conteneurs Docker :

```bash
docker compose up
```

Une fois les conteneurs prêts, accéder à l'API sur :

- **API** : http://localhost:8000
- **Swagger UI** : http://localhost:8000/docs

## Fichiers Essentiels

- `api.py` : API FastAPI principale
- `etl.py` : Pipeline ETL (orchestrateur)
- `etl_ingredient.py` : Transformation des ingrédients
- `etl_exercise.py` : Transformation des exercices
- `etl_load.py` : Chargement en base de données
- `requirements.txt` : Dépendances Python