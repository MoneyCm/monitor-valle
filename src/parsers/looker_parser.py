import datetime
import json
from typing import List, Dict, Any
from src.core.logging_config import logger
from src.core.utils import generate_record_hash

class LookerAPIParser:
    """Specialized parser for Google Looker Studio columnar JSON format."""
    
    @staticmethod
    def parse_response(data: Dict, year_tag: str = "Historical") -> List[Dict]:
        """
        Parses Google Looker Studio's columnar JSON format.
        Reconstructs rows from column-wise data arrays.
        """
        records = []
        
        try:
            # Handle both direct list and dict with dataResponse/batchedData
            responses = []
            if isinstance(data, list):
                responses = data
            else:
                responses = data.get("dataResponse") or data.get("batchedData") or []

            for resp in responses:
                # ROLE CHECK: Ignore totals and rollups that inflate the count
                role = resp.get("role", "main")
                if role not in ["main", "rowRollup0", "colRollup0"]:
                    logger.debug(f"Skipping response with role: {role}")
                    continue

                # Navigate to the dataset (Columnar Format V2)
                subsets = resp.get("dataSubset") or []
                
                for subset in subsets:
                    dataset = subset.get("dataset", {}).get("tableDataset", {})
                    columns = dataset.get("column", [])
                    if not columns:
                        continue
                    
                    # Extract values from all columns
                    col_data = []
                    for col in columns:
                        val_key = next((k for k in col.keys() if k.endswith("Column")), None)
                        if val_key:
                            col_data.append(col[val_key].get("values", []))
                    
                    if not col_data:
                        continue
                        
                    # Reconstruct rows by transposing columns
                    num_rows = len(col_data[0])
                    for i in range(num_rows):
                        record = {
                            "fecha_extraccion": datetime.datetime.now().isoformat(),
                            "fuente": "API Interna (Looker Studio)",
                            "municipio": "Jamundí",
                            "metodo_extraccion": "API_INTERNA_COLUMNAR"
                        }
                        
                        # Add generic columns
                        for col_idx, values in enumerate(col_data):
                            val = values[i] if i < len(values) else None
                            record[f"col_{col_idx}"] = val
                        
                        # Inject Year Tag for filtering context
                        record["col_9"] = year_tag
                        
                        record["hash_registro"] = generate_record_hash(record)
                        records.append(record)
            
            # Fallback for older format if no records found
            if not records and isinstance(data, dict):
                for resp in responses:
                    role = resp.get("role", "main")
                    if role not in ["main", "rowRollup0", "colRollup0"]:
                        continue

                    data_blocks = resp.get("data") or []
                    for block in data_blocks:
                        rows = block.get("row") or []
                        for row in rows:
                            cells = row.get("c") or []
                            record = {
                                "fecha_extraccion": datetime.datetime.now().isoformat(),
                                "fuente": "API Interna (Looker Studio)",
                                "municipio": "Jamundí"
                            }
                            for i, cell in enumerate(cells):
                                record[f"col_{i}"] = cell.get("v") if isinstance(cell, dict) else cell
                            
                            record["col_9"] = year_tag
                            record["hash_registro"] = generate_record_hash(record)
                            records.append(record)

        except Exception as e:
            logger.error(f"Failed to parse Looker Response: {str(e)}")
            
        return records

    def _safe_get(self, cells: List, index: int, default: Any = "") -> Any:
        try:
            val = cells[index].get("v") if isinstance(cells[index], dict) else cells[index]
            return val if val is not None else default
        except:
            return default
