import psycopg2
import os
import time
import schedule
from datetime import datetime

def get_db_connection():
    conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    return conn

def extract_data():
    """Extraction des données depuis la source"""
    print(f"[{datetime.now()}] Extraction des données...")
    # Simulation d'extraction
    data = [
        {'name': 'User A', 'email': 'usera@example.com'},
        {'name': 'User B', 'email': 'userb@example.com'}
    ]
    return data

def transform_data(data):
    """Transformation des données"""
    print(f"[{datetime.now()}] Transformation des données...")
    # Simulation de transformation
    for record in data:
        record['name'] = record['name'].upper()
        record['processed_at'] = datetime.now().isoformat()
    return data

def load_data(data):
    """Chargement des données dans la base"""
    print(f"[{datetime.now()}] Chargement des données...")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        for record in data:
            cur.execute(
                'INSERT INTO users (name, email) VALUES (%s, %s) ON CONFLICT (email) DO NOTHING;',
                (record['name'], record['email'])
            )
        
        conn.commit()
        cur.close()
        conn.close()
        print(f"[{datetime.now()}] Chargement terminé avec succès")
    except Exception as e:
        print(f"[{datetime.now()}] Erreur lors du chargement: {e}")

def run_etl():
    """Pipeline ETL complet"""
    print(f"\n{'='*50}")
    print(f"Début du processus ETL - {datetime.now()}")
    print(f"{'='*50}\n")
    
    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)
    
    print(f"\n{'='*50}")
    print(f"Fin du processus ETL - {datetime.now()}")
    print(f"{'='*50}\n")

if __name__ == '__main__':
    print("ETL Backend démarré...")
    
    # Exécuter immédiatement au démarrage
    time.sleep(5)  # Attendre que la DB soit prête
    run_etl()
    
    # Planifier l'exécution toutes les heures
    schedule.every(1).hours.do(run_etl)
    
    # Boucle infinie pour exécuter les tâches planifiées
    while True:
        schedule.run_pending()
        time.sleep(60)