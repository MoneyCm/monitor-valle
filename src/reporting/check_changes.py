"""Deteccion de cambios en los datos estadisticos para CI/CD.

Compara un hash determinista del contenido estadistico del CSV maestro
contra el hash de la ejecucion anterior. Escribe DATA_CHANGED=true/false
en $GITHUB_ENV para condicionar workflows de GitHub Actions.
"""
import hashlib
import sys
from pathlib import Path
import os
import json
from src.core.logging_config import logger
from src.core.config import settings
from src.reporting.generate_pdf_report import BoletinReporter


def _municipio_slug():
    """Genera un slug normalizado a partir del nombre del municipio."""
    return settings.obs_municipio.lower().replace("í", "i").replace("é", "e").replace("á", "a").replace("ó", "o").replace("ú", "u").replace(" ", "_").replace("-", "_")


def get_file_hash(filepath):
    """Calcula un hash MD5 del contenido estadistico normalizado del CSV.

    En lugar de hashear los bytes del archivo, re-analiza los datos a traves
    del BoletinReporter para que cambios de formato no generen falsos positivos.
    """
    try:
        reporter = BoletinReporter(filepath, "dummy.pdf", municipio=settings.obs_municipio)
        df = reporter.analyzer.load_data()
        reporter.analyzer.detect_years(df)
        reporter.analyzer.detect_corte_month(df)
        indicadores = reporter.analyzer.extract_indicadores(df)
        
        # Estructura limpia y determinista con los datos estadisticos exactos del boletin
        stats_data = {
            "current_year": str(reporter.analyzer.current_year),
            "prev_year": str(reporter.analyzer.prev_year),
            "corte_month": int(reporter.analyzer.corte_month),
            "latest_date_str": str(reporter.analyzer.latest_date_str),
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
        logger.debug(f"Datos estadisticos normalizados para hash: {data_str}")
        return hashlib.md5(data_str.encode('utf-8')).hexdigest()
    except FileNotFoundError:
        return None
    except Exception as e:
        logger.error(f"Error al calcular el hash estadistico de los datos: {e}")
        return None


def main():
    slug = _municipio_slug()
    csv_path = settings.final_dir / f"{slug}_analytics_master.csv"
    hash_file = settings.base_dir / ".github" / "data_hash.txt"
    
    if not csv_path.exists():
        logger.error("No se encontro el archivo CSV maestro.")
        sys.exit(1)
        
    current_hash = get_file_hash(csv_path)
    logger.info(f"Hash actual: {current_hash}")
    
    previous_hash = ""
    if hash_file.exists():
        previous_hash = hash_file.read_text(encoding='utf-8').strip()
        logger.info(f"Hash anterior: {previous_hash}")
        
    github_env = os.environ.get('GITHUB_ENV')
    
    if current_hash != previous_hash:
        logger.success("Se detectaron cambios en los datos respecto a la ultima ejecucion!")
        
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
