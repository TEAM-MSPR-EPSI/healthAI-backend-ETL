import os, sys, logging
from dataclasses import dataclass

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

load_dotenv()

#Logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl_load.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("LoadCSV")

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))


@dataclass
class Config:
    db_url: str = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', 5432)}/{os.getenv('DB_NAME')}"
    )


def load_csv_to_table(csv_path: str, table_name: str, engine) -> None:
    if not os.path.exists(csv_path):
        logger.error(f"Fichier CSV introuvable : {csv_path}")
        return

    df = pd.read_csv(csv_path, encoding="utf-8")
    
    if df.empty:
        logger.warning(f"CSV vide, rien à insérer dans '{table_name}' : {csv_path}")
        return
    
    logger.info(f"Chargement de {len(df)} lignes depuis {os.path.basename(csv_path)} vers '{table_name}'...")
    
    cols = ", ".join(df.columns)
    vals = ", ".join(f":{c}" for c in df.columns)
    sql = text(f"INSERT INTO {table_name} ({cols}) VALUES ({vals}) ON CONFLICT DO NOTHING")
    
    try:
        with engine.begin() as conn:
            result = conn.execute(sql, df.to_dict(orient="records"))
            inserted = result.rowcount
            skipped = len(df) - inserted
        logger.info(f"'{table_name}' — {inserted} insérée(s), {skipped} ignorée(s) (doublons).")
    except SQLAlchemyError as e:
        logger.error(f"Erreur insertion '{table_name}' : {e}")
        raise


def run() -> None:
    config = Config()
    
    try:
        engine = create_engine(config.db_url)
        logger.info(f"Connexion établie avec {config.db_url}")
        
        #Test de connexion
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Connexion vérifiée.")
        
    except SQLAlchemyError as e:
        logger.error(f"Impossible de se connecter à la base de données : {e}")
        return
    
    #Charge les CSV valides
    ingredient_csv = os.path.join(OUTPUT_DIR, "ingredient_valid.csv")
    exercise_csv = os.path.join(OUTPUT_DIR, "exercise_valid.csv")
        
    try:
        logger.info("Démarrage du chargement du CSV ingredient.")
        load_csv_to_table(ingredient_csv, "ingredient", engine)
        logger.info("Démarrage du chargement du CSV exercise.")
        load_csv_to_table(exercise_csv, "sport_exercise", engine)
        logger.info("Chargement terminé avec succès.")
    except Exception as e:
        logger.error(f"Erreur lors du chargement : {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()
