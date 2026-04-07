import asyncio
import sys
import argparse
from pathlib import Path
from playwright.async_api import async_playwright
from src.core.config import settings
from src.core.logging_config import logger
from src.scrapers.auth import AuthManager
from src.scrapers.discovery import SiteDiscovery
from src.scrapers.looker_scraper import LookerStudioScraper
from src.scrapers.reports_scraper import ReportsScraper
from src.pipelines.consolidate import ConsolidationPipeline

async def run_pipeline(args):
    """Orchestrates the full data extraction lifecycle."""
    logger.info("Starting Full Extraction Pipeline for Jamundí...")
    
    auth_mgr = AuthManager()
    
    async with async_playwright() as pw:
        # 1. Login
        try:
            page = await auth_mgr.login(pw)
            logger.info("Authentication complete.")
        except Exception as e:
            logger.critical(f"Pipeline aborted: Login failed. {str(e)}")
            return

        # 2. Site Discovery (Optional)
        if args.discover or args.full:
            logger.info("Phase 1: Discovering site modules...")
            discovery = SiteDiscovery(page)
            await discovery.discover_modules()
        
        # 3. Looker Studio Extraction
        if not args.reports_only:
            logger.info("Phase 2: Extracting data from Looker Studio dashboard...")
            looker = LookerStudioScraper(page)
            await looker.extract_dashboard_data()
        
        # 4. Mis Reportes Extraction
        if not args.dashboard_only:
            logger.info("Phase 3: Extracting data from Files/Reports section...")
            reports = ReportsScraper(page)
            await reports.scrape_all_reports()
            
        # 5. Cleanup Browser
        await auth_mgr.close()

    # 6. Consolidation & Normalization
    logger.info("Phase 4: Consolidation and Normalization...")
    consolidator = ConsolidationPipeline()
    consolidator.run()
    
    logger.info("Full pipeline execution finished successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Jamundí Data Extractor Pipeline")
    parser.add_argument("--discover", action="store_true", help="Only run site discovery")
    parser.add_argument("--full", action="store_true", default=True, help="Full extraction (Default)")
    parser.add_argument("--dashboard-only", action="store_true", help="Extract only from Looker Dashboard")
    parser.add_argument("--reports-only", action="store_true", help="Extract only from Reports section")
    
    args = parser.parse_args()
    
    try:
        asyncio.run(run_pipeline(args))
    except KeyboardInterrupt:
        logger.warning("Pipeline execution interrupted by user.")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Unexpected error in pipeline: {str(e)}")
        sys.exit(1)
