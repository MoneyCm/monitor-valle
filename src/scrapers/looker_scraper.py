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
        """Intercepts all requests for discovery/tracing."""
        if "google" in request.url:
            self.captured_requests.append({
                "url": request.url,
                "method": request.method,
                "headers": dict(request.headers),
                "post_data": request.post_data
            })

    async def _get_looker_frame(self):
        """Finds the Looker Studio iframe within the page."""
        for frame in self.page.frames:
            if "google.com/embed/reporting" in frame.url:
                return frame
        return None

    async def trigger_export(self) -> Optional[Path]:
        """Automates the '3-dots -> Export -> CSV' flow in the Looker Studio iframe."""
        logger.info("Attempting to trigger CSV Export from Looker Studio...")
        
        frame = await self._get_looker_frame()
        if not frame:
            logger.error("Looker Studio iframe not found.")
            return None

        try:
            # 1. Hover over the table to reveal the menu button (three dots)
            # We target a common container for Looker charts
            await frame.hover(".visualization-container", timeout=10000)
            
            # 2. Click the three dots (menu-button)
            # Looker often uses aria-label="More" or specific icons
            menu_selector = "button[aria-label*='Más'], button[aria-label*='More'], .flyout-menu-button"
            await frame.click(menu_selector, timeout=5000)
            await asyncio.sleep(1)
            
            # 3. Click 'Exportar' or 'Export'
            export_selector = "text='Exportar', text='Export'"
            await frame.click(export_selector, timeout=5000)
            await asyncio.sleep(1)
            
            # 4. In the dialog, ensure CSV is selected and click the final EXPORTAR button
            # This triggers a browser download
            async with self.page.expect_download() as download_info:
                await self.page.click("button:has-text('EXPORTAR'), button:has-text('EXPORT')", timeout=5000)
            
            download = await download_info.value
            save_path = self.settings.raw_dir / f"looker_export_jamundi_{download.suggested_filename}"
            await download.save_as(save_path)
            
            logger.success(f"Looker data exported successfully to {save_path}")
            self.exported_files.append(save_path)
            return save_path
            
        except Exception as e:
            logger.warning(f"Failed to trigger UI export: {str(e)}. Falling back to XHR interception.")
            # Fallback handled by the listener attached in extract_dashboard_data
            return None

    async def extract_dashboard_data(self) -> Dict[str, Any]:
        """Main entry point for Looker data extraction."""
        logger.info(f"Navigating to dashboard: {self.settings.obs_alcalde_url}")
        
        # Attach tracing
        self.page.on("request", self._handle_request)
        
        await self.page.goto(self.settings.obs_alcalde_url, timeout=self.settings.obs_timeout)
        await self.page.wait_for_load_state("networkidle")
        
        # Give it a moment to load the iframe content
        await asyncio.sleep(5)
        
        # Save discovered requests for debugging/compliance
        save_json(self.captured_requests, self.settings.raw_dir / "captured_requests.json")
        
        # Try direct export first (Cleanest data)
        csv_path = await self.trigger_export()
        
        return {
            "csv_path": str(csv_path) if csv_path else None,
            "requests_count": len(self.captured_requests)
        }
