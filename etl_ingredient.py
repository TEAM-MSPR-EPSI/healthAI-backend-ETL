import os
import re, logging, unicodedata
import pandas as pd

#Logs dans ingredient.log
logger = logging.getLogger("ETL.ingredient")

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

handler = logging.FileHandler(os.path.join(OUTPUT_DIR, "ingredient.log"), encoding="utf-8")
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


def transform(raw: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
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

    if not rows:
        logger.warning("Aucun ingredient brut trouvé.")
        return pd.DataFrame(), pd.DataFrame()

    df = pd.DataFrame(rows)
    df["ingredient_name"] = df["ingredient_name"].map(_sanitize).fillna("").str.title()
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")

    #Conserve les lignes non conformes pour export CSV au lieu de les supprimer définitivement
    name_empty = df["ingredient_name"].str.strip() == ""
    nutrients_empty = df[num_cols].isna().all(axis=1)

    invalid_df = df[name_empty | nutrients_empty].copy()
    invalid_df["rejection_reason"] = ""
    invalid_df.loc[name_empty, "rejection_reason"] = invalid_df.loc[name_empty, "rejection_reason"] + "nom_vide;"
    invalid_df.loc[nutrients_empty, "rejection_reason"] = invalid_df.loc[nutrients_empty, "rejection_reason"] + "nutriments_vides;"
    invalid_df["rejection_reason"] = invalid_df["rejection_reason"].str.rstrip(";")

    valid_df = df[~(name_empty | nutrients_empty)].copy()

    logger.info(f"ingredient prêt : {len(valid_df)} lignes valides, {len(invalid_df)} rejetées.")
    return valid_df, invalid_df


def load(valid_df: pd.DataFrame, invalid_df: pd.DataFrame, engine=None) -> None:
    valid_path = os.path.join(OUTPUT_DIR, "ingredient_valid.csv")
    invalid_path = os.path.join(OUTPUT_DIR, "ingredient_invalid.csv")

    valid_df.to_csv(valid_path, index=False, encoding="utf-8")
    invalid_df.to_csv(invalid_path, index=False, encoding="utf-8")

    logger.info(f"CSV valides écrit : {valid_path} ({len(valid_df)} lignes).")
    logger.info(f"CSV rejetés écrit : {invalid_path} ({len(invalid_df)} lignes).")

    #Insertion BDD temporairement désactivée (remplacée par export CSV)
    #if valid_df.empty:
    #    logger.warning("DataFrame vide, rien à insérer dans 'ingredient'.")
    #    return
    #    # cols = ", ".join(valid_df.columns)
    #vals = ", ".join(f":{c}" for c in valid_df.columns)
    #ON CONFLICT DO NOTHING : les doublons sont ignorés
    #sql  = text(f"INSERT INTO ingredient ({cols}) VALUES ({vals}) ON CONFLICT DO NOTHING")
    #
    #try:
    #    with engine.begin() as conn:
    #        result   = conn.execute(sql, valid_df.to_dict(orient="records"))
    #        inserted = result.rowcount
    #        skipped  = len(valid_df) - inserted
    #    logger.info(f"'ingredient' — {inserted} insérées, {skipped} ignorées (doublons).")
    #except SQLAlchemyError as e:
    #    logger.error(f"Erreur insertion 'ingredient' : {e}")
    #    raise