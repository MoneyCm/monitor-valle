import hashlib
import sys
from pathlib import Path
import os
from src.core.logging_config import logger

def get_file_hash(filepath):
    hasher = hashlib.md5()
    try:
        with open(filepath, 'rb') as f:
            buf = f.read()
            hasher.update(buf)
        return hasher.hexdigest()
    except FileNotFoundError:
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
