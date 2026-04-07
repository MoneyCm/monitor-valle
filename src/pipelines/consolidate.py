import pandas as pd
import json
import datetime
from pathlib import Path
from typing import List, Dict, Any
from src.core.config import settings
from src.core.logging_config import logger
from src.core.utils import generate_record_hash, load_json

class ConsolidationPipeline:
    """Consolidates raw JSON captures and cataloged reports into final CSV/Excel data."""
    
    def __init__(self):
        self.settings = settings
        self.raw_looker_path = settings.raw_dir / "looker_raw_capture.json"
        self.raw_reports_path = settings.raw_dir / "reports_catalog.json"

    def _normalize_looker_data(self) -> pd.DataFrame:
        """Transforms Looker Studio 'getData' payloads into a clean DataFrame."""
        if not self.raw_looker_path.exists():
            logger.warning(f"No Looker raw data found at {self.raw_looker_path}")
            return pd.DataFrame()
            
        data = load_json(self.raw_looker_path)
        # Looker Studio payloads are complex. This logic would parse dimensions and metrics.
        # For now, we build a generic skeleton for demonstration.
        
        records = []
        for packet in data:
            # Placeholder logic to extract table data from Looker responses
            # Real logic would traverse 'packet["payload"]["dataResponse"]'
            record = {
                "fecha_extraccion": datetime.datetime.now().isoformat(),
                "fuente": "Looker Studio Dashboard",
                "modulo": "Dashboard Alcalde",
                "municipio": "Jamundí",
                "anio": None, # Extract from Looker
                "mes": None,  # Extract from Looker
                "delito": "Extracted from Dashboard",
                "subcategoria": "",
                "dimension": "",
                "valor": 0,
                "unidad": "Casos",
                "url_origen": self.settings.obs_alcalde_url,
                "metodo_extraccion": "XHR Interception"
            }
            record["hash_registro"] = generate_record_hash(record)
            records.append(record)
            
        return pd.DataFrame(records)

    def _process_reports_catalog(self) -> pd.DataFrame:
        """Transforms reports metadata into a DataFrame."""
        if not self.raw_reports_path.exists():
            logger.warning(f"No reports catalog found at {self.raw_reports_path}")
            return pd.DataFrame()
            
        data = load_json(self.raw_reports_path)
        records = []
        for entry in data:
            record = {
                "fecha_extraccion": datetime.datetime.now().isoformat(),
                "fuente": "Centro de Reportes",
                "modulo": entry.get("title", "Desconocido"),
                "municipio": "Jamundí",
                "anio": entry.get("date_published", "")[:4],
                "mes": "", # Parse from date
                "delito": "Consolidado Reporte",
                "valor": 1,
                "unidad": "Archivo",
                "url_origen": entry.get("url", ""),
                "metodo_extraccion": "Crawler / Download",
                "file_path": entry.get("file_path", "")
            }
            record["hash_registro"] = generate_record_hash(record)
            records.append(record)
            
        return pd.DataFrame(records)

    def run(self):
        """Executes the full consolidation process."""
        df_looker = self._normalize_looker_data()
        df_reports = self._process_reports_catalog()
        
        # Merge all data sources
        full_df = pd.concat([df_looker, df_reports], ignore_index=True)
        
        if full_df.empty:
            logger.warning("No data found to consolidate.")
            return

        # Deduplicate
        full_df.drop_duplicates(subset=["hash_registro"], inplace=True)
        
        # Save to final CSV and Excel
        csv_path = self.settings.final_dir / "jamundi_delitos_consolidado.csv"
        excel_path = self.settings.final_dir / "jamundi_delitos_maestro.xlsx"
        
        full_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            full_df.to_excel(writer, index=False, sheet_name="General Consolidation")
            if not df_looker.empty:
                df_looker.to_excel(writer, index=False, sheet_name="Looker Dashboard Data")
            if not df_reports.empty:
                df_reports.to_excel(writer, index=False, sheet_name="Cataloged Reports Files")

        logger.success(f"Final data consolidated successfully into {csv_path} and {excel_path}")
