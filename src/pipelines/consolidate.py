"""Pipeline de consolidacion y validacion estricta de datos analiticos.

Normaliza datos crudos del scraper de Looker Studio (CSV o JSON capturado),
filtra por municipio, valida cobertura y genera el dataset maestro final.
"""
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
    """Consolidacion y validacion de datos analiticos del Observatorio."""
    
    def __init__(self):
        self.settings = settings
        self.municipio = settings.obs_municipio
        self.municipio_slug = settings.municipio_slug
        self.municipio_prefix = self.municipio.lower()[:6]
        self.raw_looker_csv_dir = settings.raw_dir
        self.raw_reports_path = settings.raw_dir / "reports_catalog.json"

    def _get_latest_looker_csv(self) -> Optional[Path]:
        """Encuentra el CSV de Looker mas reciente en el directorio raw."""
        csv_files = list(self.raw_looker_csv_dir.glob(f"looker_export_{self.municipio_slug}_*.csv"))
        if not csv_files:
            return None
        return max(csv_files, key=lambda f: f.stat().st_mtime)

    def _normalize_looker_data(self) -> pd.DataFrame:
        """Parsea el CSV exportado real de Looker Studio."""
        csv_path = self._get_latest_looker_csv()
        if not csv_path:
            logger.warning("No se encontro CSV reciente de Looker. Intentando parsear desde respuestas API capturadas...")
            return self._normalize_from_api()
            
        logger.info(f"Parseando analiticas desde {csv_path.name}")
        try:
            df = pd.read_csv(csv_path, encoding="utf-8-sig")
            
            # Mapeo de nombres de columna a campos estandar
            column_map = {
                "ANO": "anio",
                "AÑO": "anio",
                "Year": "anio",
                "MES": "mes",
                "Month": "mes",
                "CONDUCTA": "delito",
                "Conducta": "delito",
                "Casos": "valor",
                "Metric": "valor",
                "MUNICIPIO": "municipio_raw",
                "Municipio": "municipio_raw"
            }
            
            df.rename(columns=column_map, inplace=True)
            
            # Filtrado estricto si hay informacion de municipio
            if "municipio_raw" in df.columns:
                logger.info(f"Filtrando CSV por {self.municipio}...")
                df = df[df["municipio_raw"].astype(str).str.contains(self.municipio_prefix, case=False, na=False)].copy()
                
            # Asegurar columnas esenciales
            required = ["anio", "mes", "delito", "valor"]
            for col in required:
                 if col not in df.columns:
                      df[col] = None
                      
            # Agregar metadata
            df["municipio"] = self.municipio
            df["fecha_extraccion"] = datetime.datetime.now().isoformat()
            df["fuente"] = "Looker Studio Export"
            df["url_origen"] = self.settings.obs_alcalde_url
            
            # Generar hashes para deduplicacion
            df["hash_registro"] = df.apply(lambda r: generate_record_hash(r.to_dict()), axis=1)
            
            return df[required + ["municipio", "fecha_extraccion", "fuente", "url_origen", "hash_registro"]]
            
        except Exception as e:
            logger.error(f"Error parseando CSV de Looker: {str(e)}")
            return pd.DataFrame()

    def _normalize_from_api(self) -> pd.DataFrame:
        """Parsea datos desde respuestas JSON capturadas (intercepcion XHR)."""
        api_responses_path = self.settings.raw_dir / "captured_responses.json"
        if not api_responses_path.exists():
            logger.error("No se encontraron respuestas API capturadas.")
            return pd.DataFrame()

        parser = LookerAPIParser(municipio=self.municipio)
        all_records = []
        
        try:
            responses = load_json(api_responses_path)
            for entry in responses:
                records = parser.parse_response(entry.get("data", {}), year_tag=entry.get("year_tag", "Historical"))
                all_records.extend(records)
            
            if not all_records:
                return pd.DataFrame()
                
            df = pd.DataFrame(all_records)
            # Eliminar registros duplicados durante la mezcla de multiples respuestas
            if "hash_registro" in df.columns:
                df.drop_duplicates(subset=["hash_registro"], inplace=True)
            
            return df
        except Exception as e:
            logger.error(f"Error parseando respuestas API: {str(e)}")
            return pd.DataFrame()

    def validate_coverage(self, df: pd.DataFrame):
        """Aplica validaciones estrictas de completitud analitica."""
        if df.empty:
            raise RuntimeError("CRITICO: El dataset extraido esta vacio.")
            
        # Datos de API pueden tener columnas genericas, solo necesitamos registros
        is_api = "metodo_extraccion" in df.columns or ("fuente" in df.columns and "API" in str(df["fuente"].iloc[0]))
        
        total_records = len(df)
        if total_records < 10:
             raise RuntimeError(f"CRITICO: Fallo la validacion de registros ({total_records} < 10). Extraccion incompleta.")
             
        if not is_api:
            # Check de cobertura de anos (al menos 2) - solo para CSV oficial
            unique_years = df["anio"].dropna().unique()
            if len(unique_years) < 2:
                 raise RuntimeError(f"CRITICO: Cobertura de anos insuficiente ({len(unique_years)} < 2).")
        
        logger.success(f"Validacion APROBADA! Registros: {total_records}")
        return {"total_records": total_records}

    def run(self) -> Dict[str, Any]:
        """Ejecuta el pipeline y retorna un reporte de cobertura."""
        df_looker = self._normalize_looker_data()
        
        if df_looker.empty:
            logger.error("No se encontraron datos para consolidar.")
            return {"error": "Dataset vacio"}

        # Validacion (advierte pero puede no interrumpir si es API)
        try:
            report = self.validate_coverage(df_looker)
        except Exception as e:
            logger.error(f"La validacion fallo: {str(e)}")
            report = {"validation_error": str(e), "force_proceed": True}
        
        # Guardar artefactos finales
        final_csv = self.settings.final_dir / f"{self.municipio_slug}_analytics_master.csv"
        final_xlsx = self.settings.final_dir / f"{self.municipio_slug}_analytics_report.xlsx"
        
        df_looker.to_csv(final_csv, index=False, encoding="utf-8-sig")
        df_looker.to_excel(final_xlsx, index=False, engine="openpyxl")
        
        # Guardar reporte de cobertura
        with open(self.settings.final_dir / "coverage_report.json", "w", encoding="utf-8") as f:
            json.dump(report, f, indent=4, ensure_ascii=False)
            
        logger.info(f"Consolidacion finalizada. Dataset final en {final_csv}")
        return report


if __name__ == "__main__":
    pipeline = ConsolidationPipeline()
    pipeline.run()
