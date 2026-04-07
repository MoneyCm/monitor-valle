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
    """Orchestrates the full data extraction lifecycle with mandatory validation."""
    logger.info("ENVIRONMENT: Production-ready Data Extractor (Correction Total)")
    
    auth_mgr = AuthManager()
    extraction_report = {
        "timestamp": datetime.datetime.now().isoformat(),
        "status": "RUNNING",
        "phases_completed": []
    }
    
    async with async_playwright() as pw:
        # 1. Login (Mandatory)
        try:
            page = await auth_mgr.login(pw)
            logger.success("PHASE 1: Authentication Successful.")
            extraction_report["phases_completed"].append("AUTH")
        except Exception as e:
            logger.critical(f"PIPELINE BLOCKED: Login Failed. {str(e)}")
            extraction_report["status"] = "FAILED_AUTH"
            save_json(extraction_report, settings.final_dir / "pipeline_report.json")
            sys.exit(1)

        # 2. Site Discovery (Mandatory)
        logger.info("PHASE 2: Mandatory Site Discovery...")
        discovery = SiteDiscovery(page)
        routes = await discovery.discover_modules()
        extraction_report["phases_completed"].append("DISCOVERY")
        save_json(routes, settings.raw_dir / "discovered_routes.json")

        # 3. Data Extraction (Analytical Looker Studio)
        logger.info("PHASE 3: Analytical Looker Studio Extraction...")
        looker = LookerStudioScraper(page)
        result = await looker.extract_dashboard_data()
        extraction_report["phases_completed"].append("LOOKER_EXTRACTION")
        extraction_report["looker_result"] = result

        # 4. Reports Extraction (Secondary)
        if not args.dashboard_only:
            logger.info("PHASE 4: Supplementary Reports Extraction...")
            reports = ReportsScraper(page)
            await reports.scrape_all_reports()
            extraction_report["phases_completed"].append("REPORTS_CRAWLER")
            
        # 5. Cleanup Browser
        await auth_mgr.close()

    # 6. Consolidation & Strict Validation (CRITICAL)
    logger.info("PHASE 5: Consolidation & Strict Analytical Validation...")
    consolidator = ConsolidationPipeline()
    try:
        report = consolidator.run()
        logger.success("PIPELINE SUCCESS: All validation criteria met.")
        extraction_report["status"] = "SUCCESS"
        extraction_report["coverage"] = report
    except RuntimeError as re:
        logger.error(f"VALORACIÓN DE DATOS FALLIDA: {str(re)}")
        extraction_report["status"] = "FAILED_VALIDATION"
        extraction_report["error_detail"] = str(re)
        # Ensure we block the pipeline (exit with error)
        save_json(extraction_report, settings.final_dir / "pipeline_report.json")
        sys.exit(1)
        
    save_json(extraction_report, settings.final_dir / "pipeline_report.json")
    logger.info(f"Full report generated at: {settings.final_dir / 'pipeline_report.json'}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Jamundí Data Extractor Pipeline")
    parser.add_argument("--discover", action="store_true", help="Only run site discovery")
    parser.add_argument("--full", action="store_true", default=True, help="Full extraction (Default)")
    parser.add_argument("--dashboard-only", action="store_true", help="Extract only from Looker Dashboard")
    
    args = parser.parse_args()
    
    try:
        asyncio.run(run_pipeline(args))
    except KeyboardInterrupt:
        logger.warning("Pipeline execution interrupted by user.")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"FATAL: {str(e)}")
        sys.exit(1)
