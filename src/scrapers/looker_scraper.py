import json
import asyncio
from typing import List, Dict, Any, Optional
from playwright.async_api import Page, Response, Request
from src.core.config import settings
from src.core.logging_config import logger
from src.core.utils import save_json

class LookerStudioScraper:
    """Extracts data from the embedded Looker Studio dashboard via request interception."""
    
    def __init__(self, page: Page):
        self.page = page
        self.settings = settings
        self.captured_data: List[Dict[str, Any]] = []

    async def _handle_response(self, response: Response):
        """Intercepts XHR/Fetch calls from Looker Studio to catch 'getData' payloads."""
        if "google.com/embed/reporting" in response.url and "/getData" in response.url:
            logger.debug(f"Intercepted getData response: {response.url}")
            try:
                # Looker Studio data is usually in a binary/protobuf-like or structured JSON format.
                # If it's JSON, parsing is easy.
                payload = await response.json()
                self.captured_data.append({
                    "url": response.url,
                    "payload": payload,
                    "timestamp": asyncio.get_event_loop().time()
                })
            except Exception as e:
                logger.trace(f"Failed to parse JSON from Looker request: {response.url} - {str(e)}")

    async def _interact_with_filter(self, filter_name: str, option_name: str):
        """Attempts to find and select a value in a Looker Studio filter dropdown."""
        # Looker dropdowns often have 'aria-label' or title with the filter name
        logger.info(f"Targeting filter '{filter_name}' to select '{option_name}'...")
        
        # Looker often uses <iframe> or shadow DOMs. We focus on the iframe.
        frames = self.page.frames
        looker_frame = None
        for f in frames:
            if "google.com/embed/reporting" in f.url:
                looker_frame = f
                break
        
        if not looker_frame:
            logger.warning("Looker Studio iframe not found. Skipping interaction.")
            return

        try:
            # Looker Studio filters are often nested div/span with the label text
            # This is a heuristic approach since Looker DOM is extremely complex.
            # 1. Look for the filter dropdown by its label text
            filter_selector = f"text='{filter_name}'"
            await looker_frame.click(filter_selector, timeout=5000)
            await asyncio.sleep(1)
            
            # 2. Look for the option in the opened dropdown
            option_selector = f"text='{option_name}'"
            await looker_frame.click(option_selector, timeout=5000)
            logger.info(f"Successfully selected {option_name} in {filter_name}")
            
            # 3. Close the dropdown if needed (usually clicking outside or the same filter)
            await looker_frame.click(filter_selector)
        except Exception as e:
            logger.debug(f"Could not interact with filter {filter_name} via standard text: {str(e)}")

    async def apply_standard_filters(self):
        """Standard interaction to ensure Jamundí is selected and trigger data loads."""
        # This is a best-effort attempt as Looker DOM might change.
        # User requested specific filters: Municipio, Año, Delito.
        await self._interact_with_filter("Municipio", "Jamundí")
        # We can add more defaults here if needed (e.g. Current Year)
        await asyncio.sleep(2)

    async def extract_dashboard_data(self) -> List[Dict[str, Any]]:
        """Navigates to the dashboard, interacts if necessary, and captures data."""
        logger.info(f"Navigating to dashboard: {self.settings.obs_alcalde_url}")
        
        # Attach interceptor
        self.page.on("response", self._handle_response)
        
        # Navigate and wait for Looker Studio to load
        await self.page.goto(self.settings.obs_alcalde_url, timeout=self.settings.obs_timeout)
        await self.page.wait_for_load_state("networkidle")
        
        logger.info("Looker dashboard loaded. Attempting to apply filters...")
        await self.apply_standard_filters()
        
        # Often, we need to click or scroll to trigger more data loads
        logger.info("Filters applied. Waiting for iframe data...")
        await asyncio.sleep(5)  # Give it some time to fetch initial/filtered data
        
        # Capture raw data
        if not self.captured_data:
            logger.warning("No data captured via interceptor yet. Trying scroll...")
            await self.page.mouse.wheel(0, 500)
            await asyncio.sleep(5)
            
        # Log and save raw data for later parsing
        logger.info(f"Captured {len(self.captured_data)} raw data packets from Looker Studio.")
        
        # Save a sample to raw directory for debugging
        save_json(self.captured_data, self.settings.raw_dir / "looker_raw_capture.json")
        
        return self.captured_data
