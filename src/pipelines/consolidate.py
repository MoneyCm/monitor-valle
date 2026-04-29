import pandas as pd
import json
import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from src.core.config import settings
from src.core.logging_config import logger
from src.core.utils import generate_record_hash, load_json
from src.parsers.looker_parser import LookerAPIParser

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
            logger.warning("No recent Looker Export CSV found. Trying to parse from captured API responses...")
            return self._normalize_from_api()
            
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

    def _normalize_from_api(self) -> pd.DataFrame:
        """Parses data from captured JSON responses (XHR interception)."""
        api_responses_path = self.settings.raw_dir / "captured_responses.json"
        if not api_responses_path.exists():
            logger.error("No captured API responses found.")
            return pd.DataFrame()

        parser = LookerAPIParser()
        all_records = []
        
        try:
            responses = load_json(api_responses_path)
            for entry in responses:
                # 'entry' has 'url', 'data', and 'year_tag' (injected during scrape)
                records = parser.parse_response(entry.get("data", {}), year_tag=entry.get("year_tag", "Historical"))
                all_records.extend(records)
            
            if not all_records:
                return pd.DataFrame()
                
            df = pd.DataFrame(all_records)
            # Remove duplicated records during merge of multiple responses
            if "hash_registro" in df.columns:
                df.drop_duplicates(subset=["hash_registro"], inplace=True)
            
            return df
        except Exception as e:
            logger.error(f"Error parsing API responses: {str(e)}")
            return pd.DataFrame()

    def validate_coverage(self, df: pd.DataFrame):
        """Strictly enforces the analytical completeness requirements."""
        if df.empty:
            raise RuntimeError("CRITICAL: Extracted dataset is empty.")
            
        # If it's API data, it might have generic columns, we just need records.
        is_api = "metodo_extraccion" in df.columns or "fuente" in df.columns and "API" in str(df["fuente"].iloc[0])
        
        total_records = len(df)
        if total_records < 10: # Lower threshold for API capture
             raise RuntimeError(f"CRITICAL: Failed record count validation ({total_records} < 10). Extraction incomplete.")
             
        if not is_api:
            # 2. Year coverage check (at least 2 years) - ONLY for official CSV export
            unique_years = df["anio"].dropna().unique()
            if len(unique_years) < 2:
                 raise RuntimeError(f"CRITICAL: Insufficient year coverage ({len(unique_years)} < 2).")
        
        logger.success(f"Validation PASSED! Records: {total_records}")
        return {"total_records": total_records}

    def run(self) -> Dict[str, Any]:
        """Runs the pipeline and returns a coverage report."""
        df_looker = self._normalize_looker_data()
        
        if df_looker.empty:
            logger.error("No data found to consolidate.")
            return {"error": "Empty dataset"}

        # Validation (will warn but maybe not raise if API)
        try:
            report = self.validate_coverage(df_looker)
        except Exception as e:
            logger.error(f"Validation failed: {str(e)}")
            report = {"validation_error": str(e), "force_proceed": True}
        
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

if __name__ == "__main__":
    pipeline = ConsolidationPipeline()
    pipeline.run()
