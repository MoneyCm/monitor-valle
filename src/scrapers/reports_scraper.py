import os
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional
from playwright.async_api import Page, expect
from src.core.config import settings
from src.core.logging_config import logger
from src.core.utils import save_json, normalize_text

class ReportsScraper:
    """Crawls /mis-reportes and systematically downloads all historical data files."""
    
    def __init__(self, page: Page):
        self.page = page
        self.settings = settings
        self.reports_metadata: List[Dict[str, Any]] = []

    async def _crawl_pages(self) -> List[str]:
        """Navigate through the lists of reports and collect all specific report links."""
        logger.info(f"Navigating to reports repository: {self.settings.obs_reports_url}")
        
        await self.page.goto(self.settings.obs_reports_url, timeout=self.settings.obs_timeout)
        await self.page.wait_for_load_state("networkidle")
        
        # Scrape all report links. Usually these are inside '.btn-ver-reporte' or similar.
        report_links = await self.page.eval_on_selector_all(
            "a[href*='/mis-reportes/']", 
            "links => links.map(a => a.href)"
        )
        
        # Remove duplicates
        unique_links = list(set([l for l in report_links if '/mis-reportes/' in l and l != self.settings.obs_reports_url]))
        logger.info(f"Found {len(unique_links)} unique report links.")
        return unique_links

    async def _download_files_from_report(self, report_url: str):
        """Go to a report detail page and download all associated files (PDF, XLSX)."""
        logger.debug(f"Exploring report: {report_url}")
        await self.page.goto(report_url, timeout=self.settings.obs_timeout)
        await self.page.wait_for_load_state("domcontentloaded")
        
        # Extract title and date
        try:
            title = await self.page.inner_text("h1")
            date_published = await self.page.inner_text(".fecha-publicacion") if await self.page.query_selector(".fecha-publicacion") else ""
        except:
            title = f"Report_{report_url.split('/')[-1]}"
            date_published = ""

        # Identify files for download
        download_selectors = await self.page.query_selector_all("a[href*='/descargar']")
        
        for selector in download_selectors:
            # We want to name files descriptively: Year_Month_Title.ext
            filename = await selector.get_attribute("title") or await selector.inner_text()
            
            # Start download
            try:
                # With Playwright, downloads are handled via 'expect_download'
                async with self.page.expect_download() as download_info:
                    await selector.click()
                
                download_obj = await download_info.value
                ext = Path(download_obj.suggested_filename).suffix
                
                # Custom name formatting
                safe_name = "".join([c if c.isalnum() else "_" for c in normalize_text(title)])
                report_id = report_url.split("/")[-1]
                save_path = self.settings.raw_dir / f"report_{report_id}_{safe_name}{ext}"
                
                await download_obj.save_as(save_path)
                logger.info(f"Downloaded: {save_path.name}")
                
                self.reports_metadata.append({
                    "title": title,
                    "date_published": date_published,
                    "url": report_url,
                    "file_path": str(save_path),
                    "original_filename": download_obj.suggested_filename
                })
            except Exception as e:
                logger.error(f"Failed to download file from {report_url}: {str(e)}")

    async def scrape_all_reports(self):
        """Master crawl of the reports section."""
        links = await self._crawl_pages()
        for i, link in enumerate(links):
            logger.info(f"Processing report {i+1}/{len(links)}...")
            await self._download_files_from_report(link)
            # Gentle wait to avoid rate limiting
            await asyncio.sleep(2)
        
        save_json(self.reports_metadata, self.settings.raw_dir / "reports_catalog.json")
        return self.reports_metadata
