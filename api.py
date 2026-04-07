import os
import subprocess
import logging
from pathlib import Path
from typing import List

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
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

OUTPUT_DIR = Path(__file__).parent

#Fichiers CSV générés par etl.py
CSV_FILES = {
    "ingredient_valid": OUTPUT_DIR / "ingredient_valid.csv",
    "ingredient_invalid": OUTPUT_DIR / "ingredient_invalid.csv",
    "exercise_valid": OUTPUT_DIR / "exercise_valid.csv",
    "exercise_invalid": OUTPUT_DIR / "exercise_invalid.csv",
}


@app.get("/health")
async def health_check():
    return {"status": "OK", "service": "etl_backend"}


@app.post("/etl/extract-transform")
async def run_etl(background_tasks: BackgroundTasks):
    logger.info("Démarrage du pipeline ETL (etl.py)...")
    
    try:
        result = subprocess.run(
            ["python", str(OUTPUT_DIR / "etl.py")],
            cwd=str(OUTPUT_DIR),
            capture_output=True,
            text=True,
            timeout=300
        )
        
        logger.info("Pipeline ETL exécuté.")
        
        if result.returncode != 0:
            logger.error(f"Erreur ETL : {result.stderr}")
            raise HTTPException(
                status_code=500,
                detail=f"Erreur lors de l'exécution du pipeline : {result.stderr}"
            )
        
        return {
            "status": "success",
            "message": "Pipeline ETL exécuté avec succès",
            "logs": result.stdout,
        }
    
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Timeout dépassé (5 minutes)")
    except Exception as e:
        logger.error(f"Erreur non gérée : {e}")
        raise HTTPException(status_code=500, detail=f"Erreur : {str(e)}")


@app.get("/csv")
async def list_csv_files():
    csv_status = {}
    
    for name, path in CSV_FILES.items():
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
        "total_files": len(CSV_FILES),
        "csv": csv_status
    }


@app.post("/etl/load-to-db")
async def load_csv_to_db():
    logger.info("Démarrage du chargement en BDD (etl_load.py)...")
    
    try:
        result = subprocess.run(
            ["python", str(OUTPUT_DIR / "etl_load.py")],
            cwd=str(OUTPUT_DIR),
            capture_output=True,
            text=True,
            timeout=300
        )
        
        logger.info("Chargement BDD exécuté.")
        
        if result.returncode != 0:
            logger.error(f"Erreur chargement BDD : {result.stderr}")
            raise HTTPException(
                status_code=500,
                detail=f"Erreur lors du chargement en BDD : {result.stderr}"
            )
        
        return {
            "status": "success",
            "message": "Chargement en base de données exécuté avec succès",
            "logs": result.stdout,
        }
    
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Timeout dépassé (5 minutes)")
    except Exception as e:
        logger.error(f"Erreur non gérée : {e}")
        raise HTTPException(status_code=500, detail=f"Erreur : {str(e)}")


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        reload=False
    )
