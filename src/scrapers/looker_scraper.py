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
                # Safely try to get post_data as text, fallback to placeholder if binary/gzipped
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

    async def _get_looker_frame(self):
        """Finds the Looker Studio iframe within the page."""
        # Try multiple times since frames load dynamically
        for _ in range(5):
            for frame in self.page.frames:
                if "google.com/embed/reporting" in frame.url:
                    return frame
            await asyncio.sleep(2)
        return None

    async def trigger_export(self) -> Optional[Path]:
        """Automates the '3-dots -> Export -> CSV' flow in the Looker Studio iframe."""
        logger.info("Attempting to trigger CSV Export from Looker Studio UI...")
        
        frame = await self._get_looker_frame()
        if not frame:
            logger.error("Looker Studio iframe not found after waiting.")
            return None

        try:
            # 1. Wait for visualization container to be visible (Increades timeout to 30s)
            logger.debug("Waiting for visualization container...")
            viz_selector = ".visualization-container, .tool-container, g.canvas"
            await frame.wait_for_selector(viz_selector, timeout=30000)
            
            # 2. Hover over the table to reveal the menu button
            await frame.hover(viz_selector)
            await asyncio.sleep(2)
            
            # 3. Click the three dots (menu-button)
            # Looker often uses aria-label="More options" or similar
            menu_selector = "button[aria-label*='Más'], button[aria-label*='More'], .flyout-menu-button"
            await frame.click(menu_selector, timeout=10000)
            logger.debug("Table menu opened.")
            await asyncio.sleep(1)
            
            # 4. Click 'Exportar' or 'Export'
            export_selector = "text='Exportar', text='Export'"
            await frame.click(export_selector, timeout=5000)
            logger.debug("Export dialog opened.")
            await asyncio.sleep(1)
            
            # 5. In the dialog, ensure CSV is selected and click the final EXPORTAR button
            async with self.page.expect_download(timeout=60000) as download_info:
                # Final button in the export dialog
                await self.page.click("button:has-text('EXPORTAR'), button:has-text('EXPORT')", timeout=10000)
            
            download = await download_info.value
            save_path = self.settings.raw_dir / f"looker_export_jamundi_{download.suggested_filename}"
            await download.save_as(save_path)
            
            logger.success(f"Looker data exported successfully to {save_path}")
            self.exported_files.append(save_path)
            return save_path
            
        except Exception as e:
            logger.warning(f"UI export interaction failed: {str(e)}. Portal may be slow or layout changed.")
            # Take screenshot of the failure for debugging
            await self.page.screenshot(path=str(self.settings.logs_dir / "export_failure_ui.png"))
            return None

    async def extract_dashboard_data(self) -> Dict[str, Any]:
        """Main entry point for Looker data extraction."""
        logger.info(f"Navigating to dashboard: {self.settings.obs_alcalde_url}")
        
        # Attach tracing
        self.page.on("request", self._handle_request)
        
        await self.page.goto(self.settings.obs_alcalde_url, timeout=self.settings.obs_timeout)
        await self.page.wait_for_load_state("networkidle")
        
        # Extended wait for Looker iframes to initialize
        await asyncio.sleep(10)
        
        # Save discovered requests for debugging
        save_json(self.captured_requests, self.settings.raw_dir / "captured_requests.json")
        
        # Try direct export
        csv_path = await self.trigger_export()
        
        return {
            "csv_path": str(csv_path) if csv_path else None,
            "requests_count": len(self.captured_requests)
        }
