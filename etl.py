import os, re, sys, time, logging, unicodedata
from dataclasses import dataclass, field
from datetime import datetime

import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("etl.log", encoding="utf-8")]
)
logger = logging.getLogger("ETL")


@dataclass
class Config:
    db_url:  str = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', 5432)}/{os.getenv('DB_NAME')}"
    )
    timeout:  int = 30
    retries:  int = 3
    backoff:  int = 2
    apis: dict = field(default_factory=lambda: {
        "foods":     "https://world.openfoodfacts.org/api/v2/search?fields=product_name,nutriments&page_size=200&countries_tags=france&languages_tags=fr",
        "exercises": "https://wger.de/api/v2/exerciseinfo/?format=json",
    })


class ExtractError(Exception): pass
class TransformError(Exception): pass


#EXTRACT

def fetch(url: str, config: Config) -> dict:
    for attempt in range(1, config.retries + 1):
        try:
            r = requests.get(url, timeout=config.timeout)
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
 
 
def extract(url: str, config: Config, max_results: int = 200) -> list[dict]:
    data    = fetch(url, config)
    records = data.get("products") or data.get("results") or data
 
    # Pagination wger : next contient l'url de la page suivante
    while len(records) < max_results and data.get("next"):
        data     = fetch(data["next"], config)
        records += data.get("results", [])
 
    records = records[:max_results]
    logger.info(f"{len(records)} enregistrements récupérés depuis {url}.")
    return records


#TRANSFORM

def sanitize(v):
    if not isinstance(v, str) or pd.isna(v):
        return v
    try:
        v = v.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        pass
    v = unicodedata.normalize("NFC", v)
    v = "".join(" " if c in "\n\r\t" else c for c in v if not unicodedata.category(c).startswith("C") or c in "\n\r\t")
    return re.sub(r" +", " ", v).strip()


def transform_foods(raw: list[dict]) -> pd.DataFrame:
    if not raw:
        raise TransformError("Aucune donnée foods à transformer.")

    rows = []
    for item in raw:
        n = item.get("nutriments", {})
        rows.append({
            "ingredient_name":              item.get("product_name", ""),
            "ingredient_energy_100g":       n.get("energy-kcal_100g"),
            "ingredient_protein_100g":      n.get("proteins_100g"),
            "ingredient_carbohydrate_100g": n.get("carbohydrates_100g"),
            "ingredient_fats_100g":         n.get("fat_100g"),
            "ingredient_fiber_100g":        n.get("fiber_100g"),
            "ingredient_sugars_100g":       n.get("sugars_100g"),
            "ingredient_salt_100g":         n.get("salt_100g"),
            "ingredient_saturated_fats_100g": n.get("saturated-fat_100g"),
        })

    num_cols = [
        "ingredient_energy_100g", "ingredient_protein_100g", "ingredient_carbohydrate_100g",
        "ingredient_fats_100g", "ingredient_fiber_100g", "ingredient_sugars_100g",
        "ingredient_salt_100g", "ingredient_saturated_fats_100g"
    ]

    df = pd.DataFrame(rows)
    df["ingredient_name"] = df["ingredient_name"].map(sanitize).str.title()
    df = df[df["ingredient_name"].str.strip() != ""]
    df = df.dropna(subset=num_cols, how="all")
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")

    logger.info(f"ingredient prêt : {len(df)} lignes.")
    return df


def transform_exercises(raw: list[dict]) -> pd.DataFrame:
    if not raw:
        raise TransformError("Aucune donnée exercises à transformer.")

    rows = []
    for item in raw:
        # Nom et description : dans translations[], on prend le français (language == 5) en priorité
        translations = item.get("translations", [])
        lang = next((t for t in translations if t.get("language") == 5), None)
        if not lang:
            lang = next((t for t in translations if t.get("language") == 2), None)
        if not lang:
            continue

        rows.append({
            "sport_exercise_name":        lang.get("name", ""),
            "sport_exercise_instruction": lang.get("description", ""),
        })

    df = pd.DataFrame(rows)

    for col in ["sport_exercise_name", "sport_exercise_instruction"]:
        df[col] = df[col].map(sanitize)

    df["sport_exercise_name"] = df["sport_exercise_name"].str.title()
    df["sport_exercise_instruction"] = df["sport_exercise_instruction"].str.strip()
    df = df[df["sport_exercise_name"].str.strip() != ""]

    logger.info(f"sport_exercise prêt : {len(df)} lignes.")
    return df


#LOAD

def load(df: pd.DataFrame, table: str, engine) -> None:
    if df.empty:
        logger.warning(f"DataFrame vide, rien à insérer dans '{table}'.")
        return

    cols = ", ".join(df.columns)
    vals = ", ".join(f":{c}" for c in df.columns)
    sql  = text(f"INSERT INTO {table} ({cols}) VALUES ({vals}) ON CONFLICT DO NOTHING")

    try:
        with engine.begin() as conn:
            result   = conn.execute(sql, df.to_dict(orient="records"))
            inserted = result.rowcount
            skipped  = len(df) - inserted
        logger.info(f"'{table}' — {inserted} insérées, {skipped} ignorées (doublons).")
    except SQLAlchemyError as e:
        logger.error(f"Erreur insertion '{table}' : {e}")
        raise


#PIPELINE

def run(config: Config = None, engine=None) -> None:
    config = config or Config()
    start  = time.perf_counter()
    logger.info("Démarrage du pipeline.")

    try:
        engine = engine or create_engine(config.db_url, pool_pre_ping=True)

        load(transform_foods(extract(config.apis["foods"], config)),         "ingredient",     engine)
        load(transform_exercises(extract(config.apis["exercises"], config)), "sport_exercise", engine)

    except (ExtractError, TransformError, SQLAlchemyError) as e:
        logger.error(f"Pipeline interrompu : {e}")
        sys.exit(1)

    logger.info(f"Pipeline terminé en {time.perf_counter() - start:.2f}s.")


if __name__ == "__main__":
    run()