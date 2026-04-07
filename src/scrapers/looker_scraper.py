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
        """Automates the '3-dots -> Export -> CSV' flow using Frame Locator with strict mode fixes."""
        logger.info("Attempting to trigger CSV Export from Looker Studio UI...")
        
        # 1. Use Frame Locator to target the Looker Studio iframe (cross-origin safe)
        looker_frame = self.page.frame_locator('iframe[src*="lookerstudio.google.com"]')
        
        try:
            # 2. Fix 'Strict Mode Violation': Target only the HEADING for the table title
            # This identifies the specific table container reliably.
            logger.debug("Waiting for specific table heading...")
            table_heading = looker_frame.get_by_role("heading", name="Casos por Año y Mes").first
            await table_heading.wait_for(state="visible", timeout=45000)
            
            # 3. Identify and hover the specific table container
            # We look for the main visualization container containing our heading
            await table_heading.hover()
            await asyncio.sleep(2)
            
            # 4. Find the 'More Options' (3 dots) button
            # We look for buttons with specific labels, taking only the first match to avoid strict mode errors
            menu_btn = looker_frame.locator('button[aria-label*="opciones"], button[aria-label*="options"], .flyout-menu-button').first
            await menu_btn.click(timeout=10000)
            logger.debug("Table context menu opened.")
            await asyncio.sleep(2)
            
            # 5. Select 'Exportar' from the dropdown
            export_item = looker_frame.locator('text="Exportar", text="Export"').first
            await export_item.click(timeout=5000)
            logger.debug("Export dialog triggered.")
            await asyncio.sleep(2)
            
            # 6. Final confirmation in the Looker dialog
            async with self.page.expect_download(timeout=60000) as download_info:
                # Target the 'EXPORTAR' button in the popup dialog (which is also inside the frame)
                await looker_frame.locator('button:has-text("EXPORTAR"), button:has-text("EXPORT")').last.click(timeout=10000)
            
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
        
        # Extended wait for JS boot
        await asyncio.sleep(15)
        
        # Discovery info
        save_json(self.captured_requests, self.settings.raw_dir / "captured_requests.json")
        
        # Try direct export
        csv_path = await self.trigger_export()
        
        return {
            "csv_path": str(csv_path) if csv_path else None,
            "requests_count": len(self.captured_requests)
        }
