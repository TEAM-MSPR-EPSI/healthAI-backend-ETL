import re, logging, unicodedata
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

#Logs dans exercise.log
logger = logging.getLogger("ETL.exercise")

handler = logging.FileHandler("exercise.log", encoding="utf-8")
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] — %(message)s"))
logger.addHandler(handler)

API_URL = "https://wger.de/api/v2/exerciseinfo/?format=json"


def _sanitize(v):
    #Corrige l'encodage, normalise Unicode et supprime les caractères de contrôle
    if not isinstance(v, str) or pd.isna(v):
        return v
    try:
        v = v.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        pass
    v = unicodedata.normalize("NFC", v)
    v = "".join(" " if c in "\n\r\t" else c for c in v if not unicodedata.category(c).startswith("C") or c in "\n\r\t")
    return re.sub(r" +", " ", v).strip()


def transform(raw: list[dict]) -> pd.DataFrame:
    #Extrait le nom et la description en français (language=5) avec anglais en fallback (language=2)
    #Les exercices sans aucune de ces deux langues sont ignorés
    rows = []
    for item in raw:
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

    if not rows:
        logger.warning("Aucun exercice valide trouvé.")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    for col in ["sport_exercise_name", "sport_exercise_instruction"]:
        df[col] = df[col].map(_sanitize)
    df["sport_exercise_name"] = df["sport_exercise_name"].str.title()
    df = df[df["sport_exercise_name"].str.strip() != ""]

    logger.info(f"sport_exercise prêt : {len(df)} lignes.")
    return df


def load(df: pd.DataFrame, engine) -> None:
    if df.empty:
        logger.warning("DataFrame vide, rien à insérer dans 'sport_exercise'.")
        return

    cols = ", ".join(df.columns)
    vals = ", ".join(f":{c}" for c in df.columns)
    #ON CONFLICT DO NOTHING : les doublons sont ignorés
    sql  = text(f"INSERT INTO sport_exercise ({cols}) VALUES ({vals}) ON CONFLICT DO NOTHING")

    try:
        with engine.begin() as conn:
            result   = conn.execute(sql, df.to_dict(orient="records"))
            inserted = result.rowcount
            skipped  = len(df) - inserted
        logger.info(f"'sport_exercise' — {inserted} insérées, {skipped} ignorées (doublons).")
    except SQLAlchemyError as e:
        logger.error(f"Erreur insertion 'sport_exercise' : {e}")
        raise