import json
import asyncio
import re
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
        self.captured_responses: List[Dict[str, Any]] = [] 
        self.exported_files: List[Path] = []
        self.current_year_focus = "Historical" 

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

    async def _handle_response(self, response: Response):
        """Intercepts XHR responses containing the actual data blocks."""
        is_looker_data = "google" in response.url and ("get_data" in response.url or "batchedData" in response.url)
        if is_looker_data:
            try:
                if response.status == 200:
                    text = await response.text()
                    # Google often prefixes JSON with )]}' to prevent XSSI
                    if text.startswith(")]}'"):
                        text = text[4:].strip()
                    
                    payload = json.loads(text)
                    self.captured_responses.append({
                        "url": response.url,
                        "data": payload,
                        "year_tag": self.current_year_focus
                    })
                    logger.success(f"Captured data block ({len(text)} bytes) from {response.url[:50]}...")
            except Exception as e:
                logger.warning(f"Could not parse response from {response.url[:30]}: {str(e)}")

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
            await asyncio.sleep(5) # Aumentado de 2 a 5
            
            # 4. Find the 'More Options' (3 dots) button
            # We look for buttons with specific labels, taking only the first match to avoid strict mode errors
            menu_btn = looker_frame.locator('button[aria-label*="opciones"], button[aria-label*="options"], .flyout-menu-button').first
            await menu_btn.wait_for(state="visible", timeout=20000) # Espera explícita a que sea visible
            await menu_btn.click(timeout=20000) # Aumentado de 10000 a 20000
            logger.debug("Table context menu opened.")
            await asyncio.sleep(3) # Aumentado de 2 a 3
            
            # 5. Select 'Exportar' from the dropdown
            export_item = looker_frame.locator('text="Exportar", text="Export", .mat-menu-item').first
            await export_item.wait_for(state="visible", timeout=10000)
            await export_item.click(timeout=10000)
            logger.debug("Export dialog triggered.")
            await asyncio.sleep(5) # Aumentado de 2 a 5 para el diálogo
            
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
        # 0. Start from Home to fulfill "click the button" requirement
        home_url = f"{self.settings.obs_url}/home"
        logger.info(f"Navigating to Home to start via 'Informe Alcaldes' button: {home_url}")
        
        self.page.on("request", self._handle_request)
        self.page.on("response", self._handle_response)
        
        await self.page.goto(home_url, timeout=self.settings.obs_timeout)
        await self.page.wait_for_load_state("networkidle")
        
        # 1. Click 'Informe Alcaldes' button
        try:
            logger.info("Clicking 'Informe Alcaldes' button...")
            btn = self.page.get_by_role("link", name="Informe Alcaldes").first
            await btn.click(timeout=10000)
            await self.page.wait_for_load_state("networkidle")
            logger.success(f"Reached dashboard: {self.page.url}")
        except Exception as e:
            logger.warning(f"Failed to click button, navigating directly to {self.settings.obs_alcalde_url}. Error: {e}")
            await self.page.goto(self.settings.obs_alcalde_url, timeout=self.settings.obs_timeout)

        # 2. Wait for Looker Studio to initialize
        try:
            await self.page.wait_for_load_state("load", timeout=60000)
            logger.info("Waiting 20s for Looker JS/Iframes to fully initialize...")
            await asyncio.sleep(20) 
        except:
            logger.warning("Load state timeout exceeded, proceeding with best-effort capture.")

        looker_frame = self.page.frame_locator('iframe[src*="lookerstudio.google.com"]')

        # PHASE 2: Ensure Municipality filter is set to Jamundí
        logger.info(f"Setting Municipality filter specifically to: {self.settings.obs_municipio}...")
        try:
            # 1. Open the filter (Using the proven selector from previous versions)
            municipio_trigger = looker_frame.locator('div:has-text("MUNICIPIO"), div:has-text("Municipio")').last
            
            await municipio_trigger.hover()
            await asyncio.sleep(1)
            await municipio_trigger.click(force=True)
            await asyncio.sleep(3)

            # 2. Search for Jamundí in the filter's internal search box if it exists
            search_box = looker_frame.locator('input[placeholder*="Buscar"], input[placeholder*="Search"]').first
            if await search_box.is_visible(timeout=3000):
                await search_box.fill(self.settings.obs_municipio)
                await asyncio.sleep(2)

            # 3. Use 'Only' (Solo) to select only Jamundí if available, or just click it
            # Looker Studio often has a 'Solo' link on hover
            jamundi_option = looker_frame.locator('.ng2-menu-item, .mat-menu-item, div[role="option"]').filter(has_text=self.settings.obs_municipio).first
            await jamundi_option.hover()
            await asyncio.sleep(1)
            
            # Click the option itself
            await jamundi_option.click(force=True)
            logger.success(f"Municipality {self.settings.obs_municipio} selected.")
            
            await self.page.keyboard.press("Escape")
            await asyncio.sleep(5)
        except Exception as e:
            logger.warning(f"Municipality filter selection issue (might be fixed label): {str(e)}")

        # 4. Sequential selection of years to get detailed breakdowns
        years_to_capture = ["2024", "2025", "2026"]
        logger.info(f"Starting year-by-year extraction for {years_to_capture}...")
        
        for year in years_to_capture:
            try:
                self.current_year_focus = year 
                logger.info(f"Filtering by Year: {year}...")
                
                # Use a very specific selector for the button to avoid tooltips
                # Looker Studio filters are typically button.lego-control
                # We use a more flexible text match for AÑO/Año
                anio_trigger = looker_frame.locator('button.lego-control').filter(has_text=re.compile(r"AÑO|Año", re.I)).first
                
                # Check visibility and click
                if await anio_trigger.is_visible(timeout=10000):
                    await anio_trigger.hover()
                    await asyncio.sleep(1)
                    await anio_trigger.click(force=True, timeout=10000)
                else:
                    # Fallback to coordinate-based or generic text if locator fails
                    logger.warning(f"Button 'AÑO' not directly visible for year {year}, trying generic text click...")
                    target = looker_frame.get_by_text("AÑO", exact=False).first
                    await target.hover()
                    await target.click(force=True, timeout=10000)
                
                await asyncio.sleep(5)
                
                # Select the specific year from the dropdown list
                # We use a more flexible item selector
                target_year = looker_frame.locator('.ng2-menu-item, .mat-menu-item, div[role="option"], .item-label').filter(has_text=year).first
                
                if not await target_year.is_visible(timeout=5000):
                    logger.warning(f"Year {year} not found in specific classes, trying generic text...")
                    target_year = looker_frame.get_by_text(year, exact=True).first
                
                await target_year.click(force=True, timeout=10000)
                
                logger.info(f"Year {year} selected, now re-ensuring Municipality is {self.settings.obs_municipio}...")
                await asyncio.sleep(5)
                
                # Re-select Municipality to be sure (it often resets when Year changes)
                try:
                    municipio_trigger = looker_frame.locator('div:has-text("MUNICIPIO"), div:has-text("Municipio")').last
                    if await municipio_trigger.is_visible(timeout=5000):
                        await municipio_trigger.click(force=True)
                        await asyncio.sleep(2)
                        target_mun = looker_frame.locator('.ng2-menu-item, .mat-menu-item, div[role="option"]').filter(has_text=self.settings.obs_municipio).first
                        if await target_mun.is_visible(timeout=5000):
                            await target_mun.click(force=True)
                        await self.page.keyboard.press("Escape")
                except:
                    pass
                
                logger.info(f"Waiting for data refresh for {year}...")
                await asyncio.sleep(15) 
                
                logger.success(f"Captured data sequence for {year}")
                await self.page.keyboard.press("Escape")
                await asyncio.sleep(3)
            except Exception as e:
                logger.warning(f"Failed to filter for year {year}: {str(e)}")
                await self.page.screenshot(path=str(self.settings.logs_dir / f"failure_year_{year}.png"))

        # Save collective raw responses for the parser
        save_json(self.captured_responses, self.settings.raw_dir / "captured_responses.json")
        
        # Try direct export (as backup)
        csv_path = await self.trigger_export()
        
        return {
            "csv_path": str(csv_path) if csv_path else None,
            "requests_count": len(self.captured_requests),
            "responses_count": len(self.captured_responses)
        }
