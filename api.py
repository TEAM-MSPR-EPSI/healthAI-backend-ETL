import io
import subprocess
import logging
from pathlib import Path
from typing import List
import time
import csv


from etl_load import load_csv_to_table, Config as LoadConfig
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from etl import etl_ingredient, etl_exercise, run_pipeline, Config
from etl_ingredient import validate_ingredients 

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from pydantic import BaseModel
from typing import Optional
import uvicorn

#Fonction
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

#Fichiers CSV générés par etl.py
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


@app.get("/health")
async def health_check():
    return {"status": "OK", "service": "etl_backend"}


@app.post("/etl/extract-transform")
async def run_etl(background_tasks: BackgroundTasks):
    try:        
        result = subprocess.run(
            ["python", str(OUTPUT_DIR / "etl.py")],
            cwd=str(OUTPUT_DIR),
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.stdout.strip():
            logger.info(result.stdout)
        
        if result.stderr.strip():
            logger.warning(result.stderr)
        
        if result.returncode != 0:
            logger.error("  └─ ERREUR: Le script ETL a échoué\n")
            raise HTTPException(
                status_code=500,
                detail=f"Erreur lors de l'exécution du pipeline : {result.stderr}"
            )
        
        logger.info("  └─ ✓ Code de retour valide (0)\n")
        
        return {
            "status": "success",
            "message": "Pipeline ETL exécuté avec succès",
            "detailed_logs": result.stdout.strip().split('\n') if result.stdout.strip() else [],
            "return_code": result.returncode,
        } 
    
    except subprocess.TimeoutExpired:
        logger.error("[TIMEOUT] Le pipeline ETL a dépassé 300 secondes")
        logger.error("=" * 100)
        raise HTTPException(status_code=504, detail="Timeout dépassé (300 secondes)")
    except Exception as e:
        logger.error(f"[ERREUR] Exception non gérée: {e}")
        logger.error("=" * 100)
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
            size = path.stat().st_size
            csv_status[name] = {
                "exists": True,
                "size": f"{size / 1024:.2f} KB",
                "path": str(path),
            }
        else:
            csv_status[name] = {
                "exists": False,
                "message": "Fichier non généré (lancez /etl/extract-transform)"
            }
    
    return {
        "total_files": len(CSV_FILES_ALL),
        "csv": csv_status
    }
    
@app.get("/csv/ingredient")
async def list_csv_files_ingredient():
    csv_status = {}
    
    for name, path in CSV_FILES_INGREDIENT.items():
        if path.exists():
            size = path.stat().st_size
            rows = []
            with open(path, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append(row)
            csv_status[name] = {
                "exists": True,
                "size": f"{size / 1024:.2f} KB",
                "data": rows
            }
        else:
            csv_status[name] = {
                "exists": False,
                "message": "Fichier non généré (lancez /etl/extract-transform)"
            }
    
    return {
        "total_files": len(CSV_FILES_INGREDIENT),
        "csv": csv_status
    }


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

# ← remplace le PUT /csv/ingredient existant
@app.put("/csv/ingredient")
async def save_ingredient_data(payload: IngredientSaveRequest):
    rows = [row.model_dump() for row in payload.data]

    errors = validate_ingredients(rows)
    if errors:
        raise HTTPException(status_code=422, detail=errors)

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_DIR / "ingredient_valid.csv", index=False, encoding="utf-8")

    return {
        "status": "success",
        "message": f"{len(rows)} ingrédients sauvegardés",
    }


@app.get("/csv/exercise")
async def list_csv_files_exercise():
    csv_status = {}
    
    for name, path in CSV_FILES_EXERCISE.items():
        if path.exists():
            size = path.stat().st_size
            rows = []
            with open(path, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append(row)
            csv_status[name] = {
                "exists": True,
                "size": f"{size / 1024:.2f} KB",
                "data": rows
            }
        else:
            csv_status[name] = {
                "exists": False,
                "message": "Fichier non généré (lancez /etl/extract-transform)"
            }
    
    return {
        "total_files": len(CSV_FILES_EXERCISE),
        "csv": csv_status
    }     

@app.put("/csv/exercise")
async def save_exercise_data(data: dict):
    pass

@app.post("/etl/load-to-db")
async def load_csv_to_db():
    try:        
        result = subprocess.run(
            ["python", str(OUTPUT_DIR / "etl_load.py")],
            cwd=str(OUTPUT_DIR),
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.stdout.strip():
            logger.info(result.stdout)
        
        if result.stderr.strip():
            logger.warning(result.stderr)
        
        if result.returncode != 0:
            logger.error("  └─ ERREUR: Le script de chargement a échoué\n")
            raise HTTPException(
                status_code=500,
                detail=f"Erreur lors du chargement en BDD : {result.stderr}"
            )
            
        return {
            "status": "success",
            "message": "Chargement en base de données exécuté avec succès",
            "detailed_logs": result.stdout.strip().split('\n') if result.stdout.strip() else [],
            "return_code": result.returncode,
        }
    
    except subprocess.TimeoutExpired:
        logger.error("[TIMEOUT] Le chargement a dépassé 300 secondes")
        logger.error("=" * 100)
        raise HTTPException(status_code=504, detail="Timeout dépassé (300 secondes)")
    except Exception as e:
        logger.error(f"[ERREUR] Exception non gérée: {e}")
        logger.error("=" * 100)
        raise HTTPException(status_code=500, detail=f"Erreur : {str(e)}")

@app.post("/etl/load-to-db/ingredient")
async def load_ingredient_to_db():
    return _run_load_direct("ingredient", "ingredient_valid.csv")

@app.post("/etl/load-to-db/exercise")
async def load_exercise_to_db():
    return _run_load_direct("sport_exercise", "exercise_valid.csv")

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        reload=False
    )
