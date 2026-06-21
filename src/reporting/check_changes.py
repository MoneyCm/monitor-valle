import hashlib
import sys
from pathlib import Path
import os
import json
import pandas as pd
from src.core.logging_config import logger
from src.reporting.generate_pdf_report import JamundiBoletinReporter

def get_file_hash(filepath):
    try:
        # Instanciar el reporter con ruta dummy para extraer los datos normalizados
        reporter = JamundiBoletinReporter(filepath, "dummy.pdf")
        df = reporter._load_data()
        reporter._detect_years(df)
        reporter._detect_corte_month(df)
        indicadores = reporter._extract_indicadores(df)
        
        # Estructura limpia y determinista con los datos estadísticos exactos del boletín
        stats_data = {
            "current_year": str(reporter.current_year),
            "prev_year": str(reporter.prev_year),
            "corte_month": int(reporter.corte_month),
            "latest_date_str": str(reporter.latest_date_str),
            "indicadores": [
                {
                    "name": str(ind["name"]),
                    "current": int(ind["current"]),
                    "prev": int(ind["prev"]),
                    "diff": int(ind["diff"]),
                    "var": str(ind["var"])
                }
                for ind in indicadores
            ]
        }
        
        # Serializar de forma ordenada a JSON y calcular hash
        data_str = json.dumps(stats_data, sort_keys=True)
        logger.debug(f"Datos estadísticos normalizados para hash: {data_str}")
        return hashlib.md5(data_str.encode('utf-8')).hexdigest()
    except FileNotFoundError:
        return None
    except Exception as e:
        logger.error(f"Error al calcular el hash estadístico de los datos: {e}")
        return None

def main():
    csv_path = Path("data/final/jamundi_analytics_master.csv")
    hash_file = Path(".github/data_hash.txt")
    
    if not csv_path.exists():
        logger.error("No se encontró el archivo CSV maestro.")
        sys.exit(1)
        
    current_hash = get_file_hash(csv_path)
    logger.info(f"Hash actual: {current_hash}")
    
    previous_hash = ""
    if hash_file.exists():
        previous_hash = hash_file.read_text(encoding='utf-8').strip()
        logger.info(f"Hash anterior: {previous_hash}")
        
    github_env = os.environ.get('GITHUB_ENV')
    
    if current_hash != previous_hash:
        logger.success("¡Se detectaron cambios en los datos respecto a la última ejecución!")
        
        # Guardar el nuevo hash
        hash_file.parent.mkdir(parents=True, exist_ok=True)
        hash_file.write_text(current_hash, encoding='utf-8')
        
        if github_env:
            with open(github_env, 'a') as f:
                f.write("DATA_CHANGED=true\n")
    else:
        logger.info("No hay cambios en los datos.")
        if github_env:
            with open(github_env, 'a') as f:
                f.write("DATA_CHANGED=false\n")

if __name__ == "__main__":
    main()
