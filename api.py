import io
import subprocess
import logging
from pathlib import Path
from typing import List, Optional
import time
import csv

from etl_load import load_csv_to_table, Config as LoadConfig
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from etl import etl_ingredient, etl_exercise, run_pipeline, Config
from etl_ingredient import validate_ingredients
from etl_exercise import validate_exercises

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from pydantic import BaseModel
import uvicorn

#Logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] — %(message)s",
)
logger = logging.getLogger("etl_backend")

app = FastAPI(
    title="HealthAI ETL API",
    description="API pour orchestrer les pipelines ETL et charger les données",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_methods=["*"],
    allow_headers=["*"],
)

OUTPUT_DIR = Path(__file__).parent

CSV_FILES_ALL = {
    "ingredient_valid": OUTPUT_DIR / "ingredient_valid.csv",
    "ingredient_invalid": OUTPUT_DIR / "ingredient_invalid.csv",
    "exercise_valid": OUTPUT_DIR / "exercise_valid.csv",
    "exercise_invalid": OUTPUT_DIR / "exercise_invalid.csv",
}

CSV_FILES_INGREDIENT = {
    "ingredient_valid": OUTPUT_DIR / "ingredient_valid.csv",
    "ingredient_invalid": OUTPUT_DIR / "ingredient_invalid.csv",
}

CSV_FILES_EXERCISE = {
    "exercise_valid": OUTPUT_DIR / "exercise_valid.csv",
    "exercise_invalid": OUTPUT_DIR / "exercise_invalid.csv",
}


# ← fonction générique pour lire un CSV
def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


# ← fonction générique pour classifier et sauvegarder un élément
def _save_and_classify(
    row: dict,
    name_field: str,
    valid_fields: list[str],
    valid_path: Path,
    invalid_path: Path,
    valid_columns: list[str],
    invalid_columns: list[str],
) -> str:
    name = row.get(name_field, "").strip()

    # 🔍 Détection des champs invalides
    invalid_reasons = [
        f"{f} manquant ou vide"
        for f in valid_fields
        if row.get(f) in [None, "", "nan"]
    ]

    is_now_valid = len(invalid_reasons) == 0

    valid_df = pd.read_csv(valid_path, encoding="utf-8") if valid_path.exists() else pd.DataFrame(columns=valid_columns)
    invalid_df = pd.read_csv(invalid_path, encoding="utf-8") if invalid_path.exists() else pd.DataFrame(columns=invalid_columns)

    if is_now_valid:
        # retire de l'invalide
        was_in_invalid = name in invalid_df[name_field].values
        invalid_df = invalid_df[invalid_df[name_field] != name]

        # ajoute ou met à jour dans le valide
        valid_df = valid_df[valid_df[name_field] != name]
        new_row = pd.DataFrame([{f: row.get(f) for f in valid_columns}])
        valid_df = pd.concat([valid_df, new_row], ignore_index=True)

        if was_in_invalid:
            message = f"✅ '{name}' corrigé et déplacé vers la liste valide"
        else:
            message = f"✅ '{name}' ajouté à la liste valide"

    else:
        # met à jour dans l'invalide
        if name in invalid_df[name_field].values:
            for f in valid_fields:
                invalid_df.loc[invalid_df[name_field] == name, f] = row.get(f)
            action = "mis à jour"
        else:
            new_row = pd.DataFrame([{f: row.get(f) for f in invalid_columns}])
            invalid_df = pd.concat([invalid_df, new_row], ignore_index=True)
            action = "ajouté"

        message = (
            f"❌ '{name}' {action} dans la liste invalide | "
            f"Raisons: {', '.join(invalid_reasons)}"
        )

    valid_df.to_csv(valid_path, index=False, encoding="utf-8")
    invalid_df.to_csv(invalid_path, index=False, encoding="utf-8")

    return message

# ← fonction générique pour lister les CSV avec data
def _list_csv_with_data(csv_files: dict) -> dict:
    csv_status = {}
    for name, path in csv_files.items():
        if path.exists():
            size = path.stat().st_size
            csv_status[name] = {
                "exists": True,
                "size": f"{size / 1024:.2f} KB",
                "data": _read_csv(path)
            }
        else:
            csv_status[name] = {
                "exists": False,
                "message": "Fichier non généré (lancez /etl/extract-transform)"
            }
    return csv_status


def _run_etl_direct(pipeline_name: str, etl_func):
    log_stream = io.StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] — %(message)s'))

    etl_logger = logging.getLogger("ETL")
    etl_logger.addHandler(handler)

    return_code = 0
    try:
        config = Config()
        engine = None
        run_pipeline(pipeline_name, etl_func, config, engine)
    except Exception as e:
        return_code = 1
        logger.error(f"[ERREUR] {pipeline_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        etl_logger.removeHandler(handler)
        handler.close()

    logs_output = log_stream.getvalue()
    return {
        "status": "success",
        "message": f"Pipeline {pipeline_name} exécuté avec succès",
        "detailed_logs": logs_output.strip().split('\n') if logs_output.strip() else [],
        "return_code": return_code,
    }


def _run_load_direct(table_name: str, csv_filename: str):
    log_stream = io.StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] — %(message)s'))
    handler.formatter.converter = time.localtime

    load_logger = logging.getLogger("LoadCSV")
    load_logger.addHandler(handler)

    return_code = 0
    try:
        config = LoadConfig()
        engine = create_engine(config.db_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        csv_path = str(OUTPUT_DIR / csv_filename)
        load_csv_to_table(csv_path, table_name, engine)
    except SQLAlchemyError as e:
        return_code = 1
        raise HTTPException(status_code=500, detail=f"Erreur BDD : {str(e)}")
    except Exception as e:
        return_code = 1
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        load_logger.removeHandler(handler)
        handler.close()

    logs_output = log_stream.getvalue()
    return {
        "status": "success",
        "message": f"Chargement {table_name} exécuté avec succès",
        "detailed_logs": logs_output.strip().split('\n') if logs_output.strip() else [],
        "return_code": return_code,
    }


# ── Modèles Pydantic ──────────────────────────────────────────────────────────

class IngredientRow(BaseModel):
    ingredient_name: str
    ingredient_energy_100g: Optional[float] = None
    ingredient_protein_100g: Optional[float] = None
    ingredient_carbohydrate_100g: Optional[float] = None
    ingredient_fats_100g: Optional[float] = None
    ingredient_fiber_100g: Optional[float] = None
    ingredient_sugars_100g: Optional[float] = None
    ingredient_salt_100g: Optional[float] = None
    ingredient_saturated_fats_100g: Optional[float] = None
    rejection_reason: Optional[str] = None

class IngredientSaveRequest(BaseModel):
    data: list[IngredientRow]

class ExerciseRow(BaseModel):
    sport_exercise_name: str
    sport_exercise_instruction: Optional[str] = None
    rejection_reason: Optional[str] = None

class ExerciseSaveRequest(BaseModel):
    data: list[ExerciseRow]


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health_check():
    return {"status": "OK", "service": "etl_backend"}


@app.post("/etl/extract-transform")
async def run_etl(background_tasks: BackgroundTasks):
    try:
        result = subprocess.run(
            ["python", str(OUTPUT_DIR / "etl.py")],
            cwd=str(OUTPUT_DIR), capture_output=True, text=True, timeout=300
        )
        if result.stdout.strip(): logger.info(result.stdout)
        if result.stderr.strip(): logger.warning(result.stderr)
        if result.returncode != 0:
            raise HTTPException(status_code=500, detail=f"Erreur ETL : {result.stderr}")
        return {
            "status": "success",
            "message": "Pipeline ETL exécuté avec succès",
            "detailed_logs": result.stdout.strip().split('\n') if result.stdout.strip() else [],
            "return_code": result.returncode,
        }
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Timeout dépassé (300 secondes)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur : {str(e)}")


@app.post("/etl/extract-transform/ingredient")
async def run_etl_ingredient():
    return _run_etl_direct("ingredient", etl_ingredient)


@app.post("/etl/extract-transform/exercise")
async def run_etl_exercise():
    return _run_etl_direct("exercise", etl_exercise)


@app.get("/csv")
async def list_csv_files():
    csv_status = {}
    for name, path in CSV_FILES_ALL.items():
        if path.exists():
            csv_status[name] = {"exists": True, "size": f"{path.stat().st_size / 1024:.2f} KB"}
        else:
            csv_status[name] = {"exists": False, "message": "Fichier non généré"}
    return {"total_files": len(CSV_FILES_ALL), "csv": csv_status}


@app.get("/csv/ingredient")
async def list_csv_files_ingredient():
    return {"total_files": len(CSV_FILES_INGREDIENT), "csv": _list_csv_with_data(CSV_FILES_INGREDIENT)}


@app.put("/csv/ingredient")
async def save_ingredient_data(payload: IngredientSaveRequest):
    rows = [row.model_dump() for row in payload.data]
    errors = validate_ingredients(rows)
    if errors:
        raise HTTPException(status_code=422, detail=errors)

    message = _save_and_classify(
        row=rows[0],
        name_field="ingredient_name",
        valid_fields=["ingredient_energy_100g", "ingredient_protein_100g",
                      "ingredient_carbohydrate_100g", "ingredient_fats_100g",
                      "ingredient_fiber_100g", "ingredient_sugars_100g",
                      "ingredient_salt_100g", "ingredient_saturated_fats_100g"],
        valid_path=OUTPUT_DIR / "ingredient_valid.csv",
        invalid_path=OUTPUT_DIR / "ingredient_invalid.csv",
        valid_columns=["ingredient_name", "ingredient_energy_100g", "ingredient_protein_100g",
                       "ingredient_carbohydrate_100g", "ingredient_fats_100g", "ingredient_fiber_100g",
                       "ingredient_sugars_100g", "ingredient_salt_100g", "ingredient_saturated_fats_100g"],
        invalid_columns=["ingredient_name", "ingredient_energy_100g", "ingredient_protein_100g",
                         "ingredient_carbohydrate_100g", "ingredient_fats_100g", "ingredient_fiber_100g",
                         "ingredient_sugars_100g", "ingredient_salt_100g", "ingredient_saturated_fats_100g",
                         "rejection_reason"],
    )
    return {"status": "success", "message": message}


@app.get("/csv/exercise")
async def list_csv_files_exercise():
    return {"total_files": len(CSV_FILES_EXERCISE), "csv": _list_csv_with_data(CSV_FILES_EXERCISE)}


@app.put("/csv/exercise")
async def save_exercise_data(payload: ExerciseSaveRequest):
    rows = [row.model_dump() for row in payload.data]
    errors = validate_exercises(rows)
    if errors:
        raise HTTPException(status_code=422, detail=errors)

    message = _save_and_classify(
        row=rows[0],
        name_field="sport_exercise_name",
        valid_fields=["sport_exercise_instruction"],
        valid_path=OUTPUT_DIR / "exercise_valid.csv",
        invalid_path=OUTPUT_DIR / "exercise_invalid.csv",
        valid_columns=["sport_exercise_name", "sport_exercise_instruction"],
        invalid_columns=["sport_exercise_name", "sport_exercise_instruction", "rejection_reason"],
    )
    return {"status": "success", "message": message}


@app.post("/etl/load-to-db")
async def load_csv_to_db():
    try:
        result = subprocess.run(
            ["python", str(OUTPUT_DIR / "etl_load.py")],
            cwd=str(OUTPUT_DIR), capture_output=True, text=True, timeout=300
        )
        if result.stdout.strip(): logger.info(result.stdout)
        if result.stderr.strip(): logger.warning(result.stderr)
        if result.returncode != 0:
            raise HTTPException(status_code=500, detail=f"Erreur chargement : {result.stderr}")
        return {
            "status": "success",
            "message": "Chargement en base de données exécuté avec succès",
            "detailed_logs": result.stdout.strip().split('\n') if result.stdout.strip() else [],
            "return_code": result.returncode,
        }
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Timeout dépassé (300 secondes)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur : {str(e)}")


@app.post("/etl/load-to-db/ingredient")
async def load_ingredient_to_db():
    return _run_load_direct("ingredient", "ingredient_valid.csv")


@app.post("/etl/load-to-db/exercise")
async def load_exercise_to_db():
    return _run_load_direct("sport_exercise", "exercise_valid.csv")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info", reload=False)