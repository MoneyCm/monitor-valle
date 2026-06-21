"""Funciones utilitarias compartidas: I/O de JSON, hashing y normalizacion de texto."""
import json
import hashlib
from pathlib import Path
from typing import Any, Union, Dict, List
from src.core.logging_config import logger

def save_json(data: Any, path: Union[str, Path]):
    """Guarda un objeto serializable a JSON con indentacion."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logger.debug(f"JSON guardado exitosamente en {path}")
    except Exception as e:
        logger.error(f"Fallo al guardar JSON en {path}: {str(e)}")

def load_json(path: Union[str, Path]) -> Union[Dict, List]:
    """Carga datos JSON desde un archivo. Retorna {} si no existe o hay error."""
    path = Path(path)
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Fallo al cargar JSON desde {path}: {str(e)}")
        return {}

def generate_record_hash(record: Dict[str, Any], keys_to_ignore: List[str] = None) -> str:
    """Genera un hash SHA-256 de un registro para deduplicacion."""
    if keys_to_ignore is None:
        keys_to_ignore = ["fecha_extraccion", "hash_registro", "url_origen", "metodo_extraccion"]
    
    # Ordenar claves para hashing consistente
    record_copy = {k: str(v) for k, v in record.items() if k not in keys_to_ignore}
    sorted_record = json.dumps(record_copy, sort_keys=True)
    return hashlib.sha256(sorted_record.encode("utf-8")).hexdigest()

def normalize_text(text: str) -> str:
    """Normalizacion basica de texto (minusculas, sin espacios extremos)."""
    if not text:
        return ""
    return str(text).strip().lower()
