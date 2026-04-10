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
    logger.info("=" * 100)
    logger.info("DÉMARRAGE DU PIPELINE ETL (etl.py)")
    logger.info("=" * 100)
    
    try:
        logger.info("[ÉTAPE 1/4] Préparation du répertoire de travail...")
        logger.info(f"  ├─ Répertoire: {OUTPUT_DIR}")
        logger.info(f"  ├─ Script: {OUTPUT_DIR / 'etl.py'}")
        logger.info(f"  └─ Timeout: 300 secondes\n")
        
        logger.info("[ÉTAPE 2/4] Exécution du script ETL...")
        logger.info("-" * 100)
        
        result = subprocess.run(
            ["python", str(OUTPUT_DIR / "etl.py")],
            cwd=str(OUTPUT_DIR),
            capture_output=True,
            text=True,
            timeout=300
        )
        
        # ==== Logs du subprocess (messages détaillés d'etl.py) ====
        if result.stdout.strip():
            logger.info(result.stdout)
        
        if result.stderr.strip():
            logger.warning(result.stderr)
        
        logger.info("-" * 100 + "\n")
        
        logger.info("[ÉTAPE 3/4] Vérification du résultat...")
        logger.info(f"  ├─ Code de retour: {result.returncode}")
        
        if result.returncode != 0:
            logger.error("  └─ ERREUR: Le script ETL a échoué\n")
            raise HTTPException(
                status_code=500,
                detail=f"Erreur lors de l'exécution du pipeline : {result.stderr}"
            )
        
        logger.info("  └─ ✓ Code de retour valide (0)\n")
        
        logger.info("[ÉTAPE 4/4] Succès - Pipeline ETL complété")
        logger.info("=" * 100)
        logger.info("✓ Pipeline ETL terminé avec succès")
        logger.info("=" * 100)
        
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
    logger.info("=" * 100)
    logger.info("DÉMARRAGE DU CHARGEMENT EN BASE DE DONNÉES (etl_load.py)")
    logger.info("=" * 100)
    
    try:
        logger.info("[ÉTAPE 1/4] Préparation du chargement...")
        logger.info(f"  ├─ Répertoire: {OUTPUT_DIR}")
        logger.info(f"  ├─ Script: {OUTPUT_DIR / 'etl_load.py'}")
        logger.info(f"  └─ Timeout: 300 secondes\n")
        
        logger.info("[ÉTAPE 2/4] Exécution du script de chargement...")
        logger.info("-" * 100)
        
        result = subprocess.run(
            ["python", str(OUTPUT_DIR / "etl_load.py")],
            cwd=str(OUTPUT_DIR),
            capture_output=True,
            text=True,
            timeout=300
        )
        
        # ==== Logs du subprocess (messages détaillés d'etl_load.py) ====
        if result.stdout.strip():
            logger.info(result.stdout)
        
        if result.stderr.strip():
            logger.warning(result.stderr)
        
        logger.info("-" * 100 + "\n")
        
        logger.info("[ÉTAPE 3/4] Vérification du résultat...")
        logger.info(f"  ├─ Code de retour: {result.returncode}")
        
        if result.returncode != 0:
            logger.error("  └─ ERREUR: Le script de chargement a échoué\n")
            raise HTTPException(
                status_code=500,
                detail=f"Erreur lors du chargement en BDD : {result.stderr}"
            )
        
        logger.info("  └─ ✓ Code de retour valide (0)\n")
        
        logger.info("[ÉTAPE 4/4] Succès - Chargement complété")
        logger.info("=" * 100)
        logger.info("✓ Chargement en base de données terminé avec succès")
        logger.info("=" * 100)
        
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


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        reload=False
    )
