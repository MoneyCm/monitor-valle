import pandas as pd
import json
import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from src.core.config import settings
from src.core.logging_config import logger
from src.core.utils import generate_record_hash, load_json

class ConsolidationPipeline:
    """Consolidation and strict validation for analytical data from Jamundí."""
    
    def __init__(self):
        self.settings = settings
        self.raw_looker_csv_dir = settings.raw_dir
        self.raw_reports_path = settings.raw_dir / "reports_catalog.json"

    def _get_latest_looker_csv(self) -> Optional[Path]:
        """Finds the most recent Looker export CSV in the raw directory."""
        csv_files = list(self.raw_looker_csv_dir.glob("looker_export_jamundi_*.csv"))
        if not csv_files:
            return None
        return max(csv_files, key=lambda f: f.stat().st_mtime)

    def _normalize_looker_data(self) -> pd.DataFrame:
        """Parses the real exported CSV from Looker Studio."""
        csv_path = self._get_latest_looker_csv()
        if not csv_path:
            logger.warning("No recent Looker Export CSV found.")
            return pd.DataFrame()
            
        logger.info(f"Parsing analytics from {csv_path.name}")
        try:
            # Looker exports often use UTF-16 or UTF-8-SIG
            df = pd.read_csv(csv_path, encoding="utf-8-sig")
            
            # Map column names to standard fields
            # Common Looker header patterns: "AÑO", "MES", "CONDUCTA", "Casos"
            column_map = {
                "AÑO": "anio",
                "Year": "anio",
                "MES": "mes",
                "Month": "mes",
                "CONDUCTA": "delito",
                "Conducta": "delito",
                "Casos": "valor",
                "Metric": "valor"
            }
            
            # Renaming and cleaning
            df.rename(columns=column_map, inplace=True)
            
            # Ensure essential columns exist
            required = ["anio", "mes", "delito", "valor"]
            for col in required:
                 if col not in df.columns:
                      df[col] = None
                      
            # Add metadata
            df["municipio"] = "Jamundí"
            df["fecha_extraccion"] = datetime.datetime.now().isoformat()
            df["fuente"] = "Looker Studio Export"
            df["url_origen"] = self.settings.obs_alcalde_url
            
            # Generate hashes for deduplication
            df["hash_registro"] = df.apply(lambda r: generate_record_hash(r.to_dict()), axis=1)
            
            return df[required + ["municipio", "fecha_extraccion", "fuente", "url_origen", "hash_registro"]]
            
        except Exception as e:
            logger.error(f"Error parsing Looker CSV: {str(e)}")
            return pd.DataFrame()

    def validate_coverage(self, df: pd.DataFrame):
        """Strictly enforces the analytical completeness requirements."""
        if df.empty:
            raise RuntimeError("CRITICAL: Extracted dataset is empty.")
            
        # 1. Total records check
        total_records = len(df)
        if total_records < 50:
             raise RuntimeError(f"CRITICAL: Failed record count validation ({total_records} < 50). Extraction incomplete.")
             
        # 2. Year coverage check (at least 2 years)
        unique_years = df["anio"].dropna().unique()
        if len(unique_years) < 2:
             raise RuntimeError(f"CRITICAL: Insufficient year coverage ({len(unique_years)} < 2).")
             
        # 3. Crime variety check (at least 3 crimes)
        unique_crimes = df["delito"].dropna().unique()
        if len(unique_crimes) < 3:
             raise RuntimeError(f"CRITICAL: Insufficient crime variety ({len(unique_crimes)} < 3).")
             
        logger.success(f"Validation PASSED! Records: {total_records} | Years: {len(unique_years)} | Crimes: {len(unique_crimes)}")
        return {
            "total_records": total_records,
            "years_count": len(unique_years),
            "crimes_count": len(unique_crimes),
            "unique_years": [int(y) for y in unique_years if y is not None],
            "unique_crimes": list(unique_crimes)
        }

    def run(self) -> Dict[str, Any]:
        """Runs the pipeline and returns a coverage report."""
        df_looker = self._normalize_looker_data()
        
        # Validation (will raise exception on failure)
        report = self.validate_coverage(df_looker)
        
        # Save final artifacts
        final_csv = self.settings.final_dir / "jamundi_analytics_master.csv"
        final_xlsx = self.settings.final_dir / "jamundi_analytics_report.xlsx"
        
        df_looker.to_csv(final_csv, index=False, encoding="utf-8-sig")
        df_looker.to_excel(final_xlsx, index=False, engine="openpyxl")
        
        # Save coverage report
        with open(self.settings.final_dir / "coverage_report.json", "w", encoding="utf-8") as f:
            json.dump(report, f, indent=4, ensure_ascii=False)
            
        logger.info(f"Consolidation finished. Final dataset in {final_csv}")
        return report
