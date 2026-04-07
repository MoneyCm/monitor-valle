import json
import hashlib
from pathlib import Path
from typing import Any, Union, Dict, List
from src.core.logging_config import logger

def save_json(data: Any, path: Union[str, Path]):
    """Saves a JSON-serializable object to a file with indentation."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logger.debug(f"JSON saved successfully to {path}")
    except Exception as e:
        logger.error(f"Failed to save JSON to {path}: {str(e)}")

def load_json(path: Union[str, Path]) -> Union[Dict, List]:
    """Loads JSON data from a file."""
    path = Path(path)
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load JSON from {path}: {str(e)}")
        return {}

def generate_record_hash(record: Dict[str, Any], keys_to_ignore: List[str] = None) -> str:
    """Generates a SHA-256 hash for a record based on its values to help with deduplication."""
    if keys_to_ignore is None:
        keys_to_ignore = ["fecha_extraccion", "hash_registro", "url_origen"]
    
    # Sort keys to ensure consistent hashing
    record_copy = {k: str(v) for k, v in record.items() if k not in keys_to_ignore}
    sorted_record = json.dumps(record_copy, sort_keys=True)
    return hashlib.sha256(sorted_record.encode("utf-8")).hexdigest()

def normalize_text(text: str) -> str:
    """Basic text normalization (lowercase, trip whitespace)."""
    if not text:
        return ""
    return str(text).strip().lower()
