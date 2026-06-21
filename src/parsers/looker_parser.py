"""Parser especializado para el formato columnar JSON de Google Looker Studio.

Reconstruye filas a partir de datos columnares, filtra por municipio
y detecta bloques globales (agregados del departamento).
"""
import datetime
import json
from typing import List, Dict, Any
from src.core.logging_config import logger
from src.core.utils import generate_record_hash


class LookerAPIParser:
    """Parser para respuestas de la API interna de Looker Studio."""

    def __init__(self, municipio: str = "Jamundi"):
        self.municipio = municipio
        # Patron de busqueda flexible: primeros 6 caracteres sin acento
        self.municipio_prefix = municipio.lower()[:6]
    
    def parse_response(self, data: Dict, year_tag: str = "Historical") -> List[Dict]:
        """Parsea respuestas JSON de Looker Studio en formato columnar.

        Args:
            data: Payload JSON de Looker Studio.
            year_tag: Etiqueta de ano para clasificar los registros.

        Returns:
            Lista de registros diccionario planos.
        """
        records = []
        
        try:
            # Manejar tanto lista directa como dict con dataResponse/batchedData
            responses = []
            if isinstance(data, list):
                responses = data
            else:
                responses = data.get("dataResponse") or data.get("batchedData") or []

            for resp in responses:
                # CHECK DE ROL: Solo tomar datos 'main' para evitar doble conteo con totales
                role = resp.get("role", "main")
                if role != "main":
                    logger.debug(f"Omitiendo respuesta con rol: {role}")
                    continue

                # Navegar al dataset (Formato Columnar V2)
                subsets = resp.get("dataSubset") or []
                
                for subset in subsets:
                    dataset = subset.get("dataset", {}).get("tableDataset", {})
                    columns = dataset.get("column", [])
                    if not columns:
                        continue
                    
                    # Extraer valores de todas las columnas
                    col_data = []
                    for col in columns:
                        val_key = next((k for k in col.keys() if k.endswith("Column")), None)
                        if val_key:
                            col_data.append(col[val_key].get("values", []))
                    
                    if not col_data:
                        continue
                        
                    # Reconstruir filas transponiendo columnas
                    num_rows = len(col_data[0])
                    
                    # Identificar columna que contiene informacion de municipio
                    municipio_col_idx = -1
                    for idx, values in enumerate(col_data):
                        unique_vals = set(str(v).lower() for v in values if v is not None)
                        if any(self.municipio.lower() in v or self.municipio_prefix in v for v in unique_vals):
                            municipio_col_idx = idx
                            break
                    
                    is_compare = subset.get("isCompare", False)
                    compare_idx = subset.get("viewTags", {}).get("compareIndex", 0)
                    
                    if is_compare or compare_idx > 0:
                        logger.debug(f"Parseando datos de comparacion: is_compare={is_compare}, compare_idx={compare_idx}, filas={num_rows}")

                    # CHECK DE SANIDAD: Detectar bloques globales por magnitud
                    # Los totales del Valle del Cauca exceden 800, Jamundi no
                    is_global_block = False
                    for idx, values in enumerate(col_data):
                        try:
                            numeric_vals = [float(v) for v in values if str(v).replace('.','',1).isdigit()]
                            if any(v > 800 for v in numeric_vals):
                                is_global_block = True
                                break
                        except (TypeError, ValueError):
                            continue
                    
                    if is_global_block:
                        logger.debug("Descartando bloque de datos globales (valores sospechosamente altos).")
                        continue

                    for i in range(num_rows):
                        # Filtrado estricto: solo si se encontro columna de municipio
                        if municipio_col_idx != -1:
                            val = str(col_data[municipio_col_idx][i]).lower() if i < len(col_data[municipio_col_idx]) else ""
                            if self.municipio_prefix not in val:
                                continue
                        # Si no se encontro columna, se asume que el scraper ya filtro
                        
                        record = {
                            "fecha_extraccion": datetime.datetime.now().isoformat(),
                            "fuente": "API Interna (Looker Studio)",
                            "municipio": self.municipio,
                            "metodo_extraccion": "API_INTERNA_COLUMNAR",
                            "is_compare": subset.get("isCompare", False),
                            "compare_index": subset.get("viewTags", {}).get("compareIndex", 0)
                        }
                        
                        # Agregar columnas genericas
                        for col_idx, values in enumerate(col_data):
                            val = values[i] if i < len(values) else None
                            record[f"col_{col_idx}"] = val
                        
                        # Inyectar etiqueta de ano para contexto de filtrado
                        record["col_9"] = year_tag
                        
                        record["hash_registro"] = generate_record_hash(record)
                        records.append(record)
            
            # Fallback para formato antiguo si no se encontraron registros
            if not records and isinstance(data, dict):
                for resp in responses:
                    role = resp.get("role", "main")
                    if role != "main":
                        continue

                    data_blocks = resp.get("data") or []
                    for block in data_blocks:
                        rows = block.get("row") or []
                        for row in rows:
                            cells = row.get("c") or []
                            record = {
                                "fecha_extraccion": datetime.datetime.now().isoformat(),
                                "fuente": "API Interna (Looker Studio)",
                                "municipio": self.municipio
                            }
                            for i, cell in enumerate(cells):
                                record[f"col_{i}"] = cell.get("v") if isinstance(cell, dict) else cell
                            
                            record["col_9"] = year_tag
                            record["hash_registro"] = generate_record_hash(record)
                            records.append(record)

        except Exception as e:
            logger.error(f"Error al parsear respuesta de Looker: {str(e)}")
            
        return records

    @staticmethod
    def _safe_get(cells: List, index: int, default: Any = "") -> Any:
        """Obtiene un valor de celda de forma segura."""
        try:
            val = cells[index].get("v") if isinstance(cells[index], dict) else cells[index]
            return val if val is not None else default
        except (IndexError, TypeError, KeyError):
            return default
