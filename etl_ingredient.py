import os
import re, logging, unicodedata
import pandas as pd

# Logs dans ingredient.log
logger = logging.getLogger("ETL.ingredient")

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

handler = logging.FileHandler(os.path.join(OUTPUT_DIR, "ingredient.log"), encoding="utf-8")
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] — %(message)s"))
logger.addHandler(handler)

API_URL = "https://world.openfoodfacts.org/api/v2/search?fields=product_name,nutriments&page_size=200&countries_tags=france&languages_tags=fr"


def _sanitize(v):
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

    # ← dédupliquer par nom, on garde la première occurrence
    duplicates = df[df.duplicated(subset=["ingredient_name"], keep="first")]
    if not duplicates.empty:
        logger.info(f"{len(duplicates)} doublons supprimés : {duplicates['ingredient_name'].tolist()}")
    df = df.drop_duplicates(subset=["ingredient_name"], keep="first")

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


NUMERIC_FIELDS = [
    "ingredient_energy_100g",
    "ingredient_protein_100g",
    "ingredient_carbohydrate_100g",
    "ingredient_fats_100g",
    "ingredient_fiber_100g",
    "ingredient_sugars_100g",
    "ingredient_salt_100g",
    "ingredient_saturated_fats_100g",
]


def validate_ingredient(row: dict, index: int) -> list[str]:
    errors = []

    if not row.get("ingredient_name", "").strip():
        errors.append(f"Ligne {index+1} : le nom est vide")

    if len(row.get("ingredient_name", "")) > 100:
        errors.append(f"Ligne {index+1} : nom trop long (max 100 caractères)")

    for field in NUMERIC_FIELDS:
        value = row.get(field)
        if value is not None and value != "":
            try:
                v = float(value)
                if v < 0:
                    errors.append(f"Ligne {index+1} — {field} : valeur négative ({v})")
                if v > 9999.99:
                    errors.append(f"Ligne {index+1} — {field} : valeur trop grande ({v})")
            except ValueError:
                errors.append(f"Ligne {index+1} — {field} : valeur non numérique ({value})")

    return errors


def validate_ingredients(rows: list[dict]) -> list[str]:
    errors = []
    for i, row in enumerate(rows):
        errors.extend(validate_ingredient(row, i))
    return errors