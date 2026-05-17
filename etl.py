import os
import sys
import time
import logging
from dataclasses import dataclass

import requests
from dotenv import load_dotenv

import etl_ingredient
import etl_exercise

load_dotenv()

# Log en console uniquement (chaque module gère son propre fichier de log)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ETL")


# Connexion BDD
@dataclass
class Config:
    db_url:  str = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', 5432)}/{os.getenv('DB_NAME')}"
    )
    timeout: int = 30
    retries: int = 3
    backoff: int = 2


class ExtractError(Exception): pass


# Appel HTTP avec retry (on réessaie plusieurs fois en cas d'échec) et backoff exponentiel (temps d'attente qui augmente à chaque tentative)
def fetch(url: str, config: Config) -> dict:
    for attempt in range(1, config.retries + 1):
        try:
            auth = None
            if "openfoodfacts.org" in url:
                headers = {"User-Agent": "HealthAI-ETL - Windows - Version 1.0"}               
                # Récupérer les credentials depuis les variables d'environnement
                api_user = os.getenv("API_USER")
                api_pass = os.getenv("API_PASSWORD")
                if api_user and api_pass:
                    auth = (api_user, api_pass)
            else:
                headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"}
            r = requests.get(url, timeout=config.timeout, headers=headers, auth=auth)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout (tentative {attempt}/{config.retries}).")
        except requests.exceptions.HTTPError as e:
            raise ExtractError(f"Erreur HTTP {e.response.status_code}.") from e
        except requests.exceptions.JSONDecodeError:
            raise ExtractError("Réponse non JSON.") from None
        except requests.exceptions.RequestException as e:
            logger.warning(f"Erreur réseau (tentative {attempt}/{config.retries}) : {e}")
        time.sleep(config.backoff ** attempt)
    raise ExtractError(f"Echec après {config.retries} tentatives.")


# Pagination automatique jusqu'à max_results enregistrements
def extract(url: str, config: Config, max_results: int = 200) -> list[dict]:
    data = fetch(url, config)
    records = data.get("products") or data.get("results") or data

    while len(records) < max_results and data.get("next"):
        data = fetch(data["next"], config)
        records += data.get("results", [])

    records = records[:max_results]
    logger.info(f"{len(records)} enregistrements récupérés.")
    return records


# Exécution d'un pipeline : extract -> transform -> load
# Si un ETL échoue, le suivant est quand meme exécuté
def run_pipeline(name: str, etl_module, config: Config, engine) -> None:
    mod_logger = logging.getLogger(f"ETL.{name}")
    mod_logger.info(f"Démarrage du pipeline ETL ({name}).")
    start = time.perf_counter()
    try:
        raw = extract(etl_module.API_URL, config)
        valid_df, invalid_df = etl_module.transform(raw)
        etl_module.load(valid_df, invalid_df, engine)
    except ExtractError as e:
        mod_logger.error(f"Pipeline interrompu : {e}")
    except Exception as e:
        mod_logger.error(f"Pipeline interrompu : {e}")
    finally:
        mod_logger.info(f"Pipeline ETL terminé en {time.perf_counter() - start:.2f}s.")


def run() -> None:
    config = Config()

    # Insertion BDD temporairement désactivée (remplacée par export CSV)
    engine = None

    run_pipeline("ingredient", etl_ingredient, config, engine)
    run_pipeline("exercise",   etl_exercise,   config, engine)


if __name__ == "__main__":
    run()
