import re, logging, unicodedata
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

#Logs dans ingredient.log
logger = logging.getLogger("ETL.ingredient")

handler = logging.FileHandler("ingredient.log", encoding="utf-8")
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] — %(message)s"))
logger.addHandler(handler)

API_URL = "https://world.openfoodfacts.org/api/v2/search?fields=product_name,nutriments&page_size=200&countries_tags=france&languages_tags=fr"


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
    #Aplatit le JSON nutriments et nettoie les données
    rows = []
    for item in raw:
        n = item.get("nutriments", {})
        rows.append({
            "ingredient_name":                item.get("product_name", ""),
            "ingredient_energy_100g":         n.get("energy-kcal_100g"),
            "ingredient_protein_100g":        n.get("proteins_100g"),
            "ingredient_carbohydrate_100g":   n.get("carbohydrates_100g"),
            "ingredient_fats_100g":           n.get("fat_100g"),
            "ingredient_fiber_100g":          n.get("fiber_100g"),
            "ingredient_sugars_100g":         n.get("sugars_100g"),
            "ingredient_salt_100g":           n.get("salt_100g"),
            "ingredient_saturated_fats_100g": n.get("saturated-fat_100g"),
        })

    num_cols = [
        "ingredient_energy_100g", "ingredient_protein_100g", "ingredient_carbohydrate_100g",
        "ingredient_fats_100g", "ingredient_fiber_100g", "ingredient_sugars_100g",
        "ingredient_salt_100g", "ingredient_saturated_fats_100g"
    ]

    df = pd.DataFrame(rows)
    df["ingredient_name"] = df["ingredient_name"].map(_sanitize).str.title()
    #Supprime les lignes sans nom et celles où tous les nutriments sont null
    df = df[df["ingredient_name"].str.strip() != ""]
    df = df.dropna(subset=num_cols, how="all")
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")

    logger.info(f"ingredient prêt : {len(df)} lignes.")
    return df


def load(df: pd.DataFrame, engine) -> None:
    if df.empty:
        logger.warning("DataFrame vide, rien à insérer dans 'ingredient'.")
        return

    cols = ", ".join(df.columns)
    vals = ", ".join(f":{c}" for c in df.columns)
    #ON CONFLICT DO NOTHING : les doublons sont ignorés
    sql  = text(f"INSERT INTO ingredient ({cols}) VALUES ({vals}) ON CONFLICT DO NOTHING")

    try:
        with engine.begin() as conn:
            result   = conn.execute(sql, df.to_dict(orient="records"))
            inserted = result.rowcount
            skipped  = len(df) - inserted
        logger.info(f"'ingredient' — {inserted} insérées, {skipped} ignorées (doublons).")
    except SQLAlchemyError as e:
        logger.error(f"Erreur insertion 'ingredient' : {e}")
        raise