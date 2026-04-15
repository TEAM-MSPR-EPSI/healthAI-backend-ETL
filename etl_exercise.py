import os
import re, logging, unicodedata
import pandas as pd
from html.parser import HTMLParser

logger = logging.getLogger("ETL.exercise")

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

handler = logging.FileHandler(os.path.join(OUTPUT_DIR, "exercise.log"), encoding="utf-8")
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] — %(message)s"))
logger.addHandler(handler)

API_URL = "https://wger.de/api/v2/exerciseinfo/?format=json"


class _HTMLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_data(self, data):
        self.parts.append(data)

    def get_text(self):
        return " ".join(self.parts)


def _strip_html(v: str) -> str:
    if not isinstance(v, str):
        return v
    # remplace les balises de liste par des tirets pour garder la structure
    v = re.sub(r"<li[^>]*>", "- ", v)
    # remplace les balises de paragraphe/bloc par des espaces
    v = re.sub(r"</(p|div|ol|ul|br)[^>]*>", " ", v)
    v = re.sub(r"<br\s*/?>", " ", v)
    # supprime &nbsp; et autres entités HTML courantes
    v = v.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    # supprime toutes les balises restantes
    stripper = _HTMLStripper()
    stripper.feed(v)
    text = stripper.get_text()
    # nettoie les espaces multiples
    return re.sub(r" +", " ", text).strip()


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

    # ← nettoyage HTML sur les instructions
    df["sport_exercise_instruction"] = df["sport_exercise_instruction"].map(_strip_html)

    df["sport_exercise_name"] = df["sport_exercise_name"].str.title()

    duplicates = df[df.duplicated(subset=["sport_exercise_name"], keep="first")]
    if not duplicates.empty:
        logger.info(f"{len(duplicates)} doublons supprimés : {duplicates['sport_exercise_name'].tolist()}")
    df = df.drop_duplicates(subset=["sport_exercise_name"], keep="first")

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


def validate_exercise(row: dict, index: int) -> list[str]:
    errors = []

    if not row.get("sport_exercise_name", "").strip():
        errors.append(f"Ligne {index+1} : le nom est vide")

    if len(row.get("sport_exercise_name", "")) > 200:
        errors.append(f"Ligne {index+1} : nom trop long (max 200 caractères)")

    return errors


def validate_exercises(rows: list[dict]) -> list[str]:
    errors = []
    for i, row in enumerate(rows):
        errors.extend(validate_exercise(row, i))
    return errors