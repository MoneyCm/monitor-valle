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
                # ROLE CHECK: ONLY take 'main' data to avoid double-counting with totals/rollups
                role = resp.get("role", "main")
                if role != "main":
                    logger.debug(f"Skipping non-main response with role: {role}")
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
                    
                    # Identify if any column contains municipality information to filter strictly
                    municipio_col_idx = -1
                    for idx, values in enumerate(col_data):
                        unique_vals = set(str(v) for v in values if v is not None)
                        if "Jamundí" in unique_vals or "JAMUNDÍ" in unique_vals:
                            municipio_col_idx = idx
                            break

                    is_compare = subset.get("isCompare", False)
                    compare_idx = subset.get("viewTags", {}).get("compareIndex", 0)
                    
                    if is_compare or compare_idx > 0:
                        logger.debug(f"Parsing comparison data: is_compare={is_compare}, compare_idx={compare_idx}, rows={num_rows}")

                    # SANITY CHECK: Detect global Valle del Cauca blocks by value magnitude
                    # In Jamundí, homicide/extortion totals are never > 500 per year. 
                    # If we see thousands, it's a global block.
                    is_global_block = False
                    for idx, values in enumerate(col_data):
                        # Check numeric columns for suspicious magnitudes
                        try:
                            numeric_vals = [float(v) for v in values if str(v).replace('.','',1).isdigit()]
                            if any(v > 800 for v in numeric_vals): # Threshold for global data
                                is_global_block = True
                                break
                        except:
                            continue
                    
                    if is_global_block:
                        logger.debug(f"Discarding global data block (suspiciously high values).")
                        continue

                    for i in range(num_rows):
                        # Strict filtering: ONLY if we found a municipality column
                        if municipio_col_idx != -1:
                            val = str(col_data[municipio_col_idx][i]) if i < len(col_data[municipio_col_idx]) else ""
                            if "Jamund" not in val:
                                continue
                        # If no column found, we assume the scraper already filtered the session

                        record = {
                            "fecha_extraccion": datetime.datetime.now().isoformat(),
                            "fuente": "API Interna (Looker Studio)",
                            "municipio": "Jamundí",
                            "metodo_extraccion": "API_INTERNA_COLUMNAR",
                            "is_compare": subset.get("isCompare", False),
                            "compare_index": subset.get("viewTags", {}).get("compareIndex", 0)
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
