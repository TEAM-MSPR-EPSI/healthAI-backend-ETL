import os
import re, logging, unicodedata
import pandas as pd

#Logs dans exercise.log
logger = logging.getLogger("ETL.exercise")

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

handler = logging.FileHandler(os.path.join(OUTPUT_DIR, "exercise.log"), encoding="utf-8")
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


def transform(raw: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    #Extrait le nom et la description en français (language=5) avec anglais en fallback (language=2)
    #Les exercices sans aucune de ces deux langues sont ignorés
    rows = []
    rejected_rows = []
    for item in raw:
        translations = item.get("translations", [])
        lang = next((t for t in translations if t.get("language") == 5), None)
        if not lang:
            lang = next((t for t in translations if t.get("language") == 2), None)
        if not lang:
            rejected_rows.append({
                "sport_exercise_name": "",
                "sport_exercise_instruction": "",
                "rejection_reason": "traduction_fr_en_absente",
            })
            continue
        rows.append({
            "sport_exercise_name":        lang.get("name", ""),
            "sport_exercise_instruction": lang.get("description", ""),
        })

    if not rows:
        logger.warning("Aucun exercice valide trouvé.")
        return pd.DataFrame(), pd.DataFrame(rejected_rows)

    df = pd.DataFrame(rows)
    for col in ["sport_exercise_name", "sport_exercise_instruction"]:
        df[col] = df[col].map(_sanitize).fillna("")
    df["sport_exercise_name"] = df["sport_exercise_name"].str.title()
    name_empty = df["sport_exercise_name"].str.strip() == ""
    invalid_from_name = df[name_empty].copy()
    if not invalid_from_name.empty:
        invalid_from_name["rejection_reason"] = "nom_exercice_vide"

    valid_df = df[~name_empty].copy()
    invalid_df = pd.concat([pd.DataFrame(rejected_rows), invalid_from_name], ignore_index=True)

    logger.info(f"sport_exercise prêt : {len(valid_df)} lignes valides, {len(invalid_df)} rejetées.")
    return valid_df, invalid_df


def load(valid_df: pd.DataFrame, invalid_df: pd.DataFrame, engine=None) -> None:
    valid_path = os.path.join(OUTPUT_DIR, "exercise_valid.csv")
    invalid_path = os.path.join(OUTPUT_DIR, "exercise_invalid.csv")

    valid_df.to_csv(valid_path, index=False, encoding="utf-8")
    invalid_df.to_csv(invalid_path, index=False, encoding="utf-8")

    logger.info(f"CSV valides écrit : {valid_path} ({len(valid_df)} lignes).")
    logger.info(f"CSV rejetés écrit : {invalid_path} ({len(invalid_df)} lignes).")

    #Insertion BDD temporairement désactivée (remplacée par export CSV)
    #if valid_df.empty:
    #    logger.warning("DataFrame vide, rien à insérer dans 'sport_exercise'.")
    #    return
    #
    #cols = ", ".join(valid_df.columns)
    #vals = ", ".join(f":{c}" for c in valid_df.columns)
    #ON CONFLICT DO NOTHING : les doublons sont ignorés
    #sql  = text(f"INSERT INTO sport_exercise ({cols}) VALUES ({vals}) ON CONFLICT DO NOTHING")
    #
    #try:
    #    with engine.begin() as conn:
    #        result   = conn.execute(sql, valid_df.to_dict(orient="records"))
    #        inserted = result.rowcount
    #        skipped  = len(valid_df) - inserted
    #    logger.info(f"'sport_exercise' — {inserted} insérées, {skipped} ignorées (doublons).")
    #except SQLAlchemyError as e:
    #    logger.error(f"Erreur insertion 'sport_exercise' : {e}")
    #    raise