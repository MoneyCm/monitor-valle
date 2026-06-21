"""Pipeline principal de extraccion completa de datos.

Orquesta: autenticacion -> descubrimiento -> extraccion Looker -> consolidacion.
"""
import asyncio
import sys
import argparse
import datetime
from pathlib import Path
from playwright.async_api import async_playwright
from src.core.config import settings
from src.core.logging_config import logger
from src.scrapers.auth import AuthManager
from src.scrapers.discovery import SiteDiscovery
from src.scrapers.looker_scraper import LookerStudioScraper
from src.scrapers.reports_scraper import ReportsScraper
from src.pipelines.consolidate import ConsolidationPipeline
from src.core.utils import save_json

async def run_pipeline(args):
    """Orquesta el ciclo completo de extraccion de datos con validacion obligatoria."""
    logger.info("ENTORNO: Extractor de datos listo para produccion")
    
    auth_mgr = AuthManager()
    extraction_report = {
        "timestamp": datetime.datetime.now().isoformat(),
        "status": "EN_EJECUCION",
        "fases_completadas": []
    }
    
    async with async_playwright() as pw:
        # 1. Autenticacion (Obligatorio)
        try:
            page = await auth_mgr.login(pw)
            logger.success("FASE 1: Autenticacion exitosa.")
            extraction_report["fases_completadas"].append("AUTH")
        except Exception as e:
            logger.critical(f"PIPELINE BLOQUEADO: Login fallido. {str(e)}")
            extraction_report["status"] = "FALLO_AUTH"
            save_json(extraction_report, settings.final_dir / "pipeline_report.json")
            sys.exit(1)

        # 2. Descubrimiento del sitio (Obligatorio)
        logger.info("FASE 2: Descubrimiento del sitio...")
        discovery = SiteDiscovery(page)
        routes = await discovery.discover_modules()
        extraction_report["fases_completadas"].append("DISCOVERY")
        save_json(routes, settings.raw_dir / "discovered_routes.json")

        # 3. Extraccion de datos (Looker Studio)
        logger.info("FASE 3: Extraccion analitica de Looker Studio...")
        looker = LookerStudioScraper(page)
        result = await looker.extract_dashboard_data()
        extraction_report["fases_completadas"].append("LOOKER_EXTRACTION")
        extraction_report["looker_result"] = result

        # 4. Extraccion de reportes (Secundario)
        if not args.dashboard_only:
            logger.info("FASE 4: Extraccion complementaria de reportes...")
            reports = ReportsScraper(page)
            await reports.scrape_all_reports()
            extraction_report["fases_completadas"].append("REPORTS_CRAWLER")
            
        # 5. Cierre del navegador
        await auth_mgr.close()

    # 6. Consolidacion y validacion (CRITICO)
    logger.info("FASE 5: Consolidacion y validacion analitica...")
    consolidator = ConsolidationPipeline()
    try:
        report = consolidator.run()
        logger.success("PIPELINE EXITOSO: Todos los criterios de validacion cumplidos.")
        extraction_report["status"] = "EXITO"
        extraction_report["coverage"] = report
    except RuntimeError as re:
        logger.error(f"VALIDACION DE DATOS FALLIDA: {str(re)}")
        extraction_report["status"] = "FALLO_VALIDACION"
        extraction_report["error_detail"] = str(re)
        save_json(extraction_report, settings.final_dir / "pipeline_report.json")
        sys.exit(1)
        
    save_json(extraction_report, settings.final_dir / "pipeline_report.json")
    logger.info(f"Reporte completo generado en: {settings.final_dir / 'pipeline_report.json'}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f"Pipeline de extraccion de datos - {settings.obs_municipio}")
    parser.add_argument("--discover", action="store_true", help="Solo ejecutar descubrimiento del sitio")
    parser.add_argument("--full", action="store_true", default=True, help="Extraccion completa (por defecto)")
    parser.add_argument("--dashboard-only", action="store_true", help="Extraer solo desde Looker Dashboard")
    
    args = parser.parse_args()
    
    try:
        asyncio.run(run_pipeline(args))
    except KeyboardInterrupt:
        logger.warning("Ejecucion del pipeline interrumpida por el usuario.")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"FATAL: {str(e)}")
        sys.exit(1)
