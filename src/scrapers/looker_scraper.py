import json
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional
from playwright.async_api import Page, Response, Request, expect
from src.core.config import settings
from src.core.logging_config import logger
from src.core.utils import save_json

class LookerStudioScraper:
    """Extracts analytical data from Looker Studio using direct Export interaction and XHR interception."""
    
    def __init__(self, page: Page):
        self.page = page
        self.settings = settings
        self.captured_requests: List[Dict[str, Any]] = []
        self.exported_files: List[Path] = []

    async def _handle_request(self, request: Request):
        """Intercepts all requests for discovery/tracing. Handles binary post_data safely."""
        if "google" in request.url:
            post_data = None
            try:
                if request.post_data:
                    post_data = request.post_data
            except Exception:
                post_data = "<BINARY_OR_COMPRESSED_DATA>"

            self.captured_requests.append({
                "url": request.url,
                "method": request.method,
                "headers": dict(request.headers),
                "post_data": post_data
            })

    async def trigger_export(self) -> Optional[Path]:
        """Automates the '3-dots -> Export -> CSV' flow using Frame Locator for cross-origin resilience."""
        logger.info("Attempting to trigger CSV Export from Looker Studio UI...")
        
        # 1. Use Frame Locator to target the Looker Studio iframe (cross-origin safe)
        looker_frame = self.page.frame_locator('iframe[src*="lookerstudio.google.com"]')
        
        try:
            # 2. Wait for the report to load common elements (Increased to 45s)
            logger.debug("Waiting for Looker Studio report content...")
            await looker_frame.locator('text="Casos por Año y Mes"').wait_for(state="visible", timeout=45000)
            
            # 3. Identify and hover the specific table to reveal its 'More' button
            # We use a locator that includes the table title to be precise
            table_container = looker_frame.locator('div:has-text("Casos por Año y Mes")').last
            await table_container.hover()
            await asyncio.sleep(2)
            
            # 4. Find the 'More Options' (3 dots) button within that table's context
            # Looker often uses aria-label="Más opciones" or "More options"
            menu_btn = looker_frame.locator('button[aria-label*="opciones"], button[aria-label*="options"], .flyout-menu-button').first
            await menu_btn.click(timeout=10000)
            logger.debug("Table context menu opened.")
            await asyncio.sleep(2)
            
            # 5. Select 'Exportar' from the dropdown
            # We use 'text' filter to find the menu item
            export_item = looker_frame.locator('text="Exportar", text="Export"').first
            await export_item.click(timeout=5000)
            logger.debug("Export dialog triggered.")
            await asyncio.sleep(2)
            
            # 6. Final confirmation in the Looker dialog (This is usually a global dialog in the frame)
            # We must wait for the download to start
            async with self.page.expect_download(timeout=60000) as download_info:
                # Target the 'EXPORTAR' button in the popup dialog
                await looker_frame.locator('button:has-text("EXPORTAR"), button:has-text("EXPORT")').click(timeout=10000)
            
            download = await download_info.value
            save_path = self.settings.raw_dir / f"looker_export_jamundi_{download.suggested_filename}"
            await download.save_as(save_path)
            
            logger.success(f"Looker data exported successfully to {save_path}")
            self.exported_files.append(save_path)
            return save_path
            
        except Exception as e:
            logger.warning(f"UI export interaction failed: {str(e)}.")
            # Screenshot of the failure
            await self.page.screenshot(path=str(self.settings.logs_dir / "export_failure_report.png"))
            return None

    async def extract_dashboard_data(self) -> Dict[str, Any]:
        """Main entry point for Looker data extraction."""
        logger.info(f"Navigating to dashboard: {self.settings.obs_alcalde_url}")
        
        self.page.on("request", self._handle_request)
        
        await self.page.goto(self.settings.obs_alcalde_url, timeout=self.settings.obs_timeout)
        await self.page.wait_for_load_state("networkidle")
        
        # Initial wait for JS boot inside the portal
        await asyncio.sleep(15)
        
        # Discovery info
        save_json(self.captured_requests, self.settings.raw_dir / "captured_requests.json")
        
        # Try direct export
        csv_path = await self.trigger_export()
        
        return {
            "csv_path": str(csv_path) if csv_path else None,
            "requests_count": len(self.captured_requests)
        }
