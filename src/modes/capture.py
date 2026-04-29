import json
import asyncio
from typing import List, Dict, Any
from playwright.async_api import async_playwright, Request, Response
from src.core.config import settings
from src.core.logging_config import logger
from src.core.utils import save_json
from src.scrapers.auth import AuthManager

class CaptureMode:
    """Technical discovery mode: intercepts and catalogs all internal API traffic."""
    
    def __init__(self):
        self.settings = settings
        self.captured_requests: List[Dict[str, Any]] = []
        self.useful_keywords = ["delito", "municipio", "anio", "mes", "reporte", "estadistica", "getData", "batchedData"]

    async def _on_request(self, request: Request):
        """Intercepts outgoing requests."""
        if request.resource_type in ["fetch", "xhr"]:
            post_data = None
            try:
                post_data = request.post_data
            except:
                pass

            req_data = {
                "url": request.url,
                "method": request.method,
                "headers": dict(request.headers),
                "payload": post_data,
                "type": request.resource_type
            }
            self.captured_requests.append(req_data)
            logger.debug(f"Captured {request.method} request: {request.url[:100]}...")

    async def run(self):
        logger.info("Initializing CAPTURE MODE...")
        auth_mgr = AuthManager()
        
        async with async_playwright() as pw:
            # 1. Login
            page = await auth_mgr.login(pw)
            
            # 2. Attach Interceptor
            page.on("request", self._on_request)
            
            # 3. Explore Dashboards
            paths = ["/alcalde", "/territorial", "/mis-reportes"]
            for path in paths:
                url = f"{self.settings.obs_url}{path}"
                logger.info(f"Analyzing path: {url}")
                await page.goto(url, timeout=self.settings.obs_timeout)
                await page.wait_for_load_state("networkidle")
                await asyncio.sleep(5)  # Allow dynamic loads
                
                # Check for iframes (Looker Studio)
                for frame in page.frames:
                    if "google.com" in frame.url:
                        logger.info(f"Looker iframe detected: {frame.url[:100]}")
            
            # 4. Filter and Save
            logger.info(f"Total requests captured: {len(self.captured_requests)}")
            
            useful = []
            for req in self.captured_requests:
                if any(kw.lower() in req["url"].lower() or (req["payload"] and kw.lower() in req["payload"].lower()) for kw in self.useful_keywords):
                    useful.append(req)
            
            save_json(self.captured_requests, self.settings.raw_dir / "captured_requests.json")
            save_json(useful, self.settings.raw_dir / "useful_endpoints.json")
            
            # Save Cookies for Extract Mode
            cookies = await page.context.cookies()
            save_json(cookies, self.settings.raw_dir / "session_cookies.json")
            
            logger.success(f"Capture complete. {len(useful)} useful endpoints identified.")
            await auth_mgr.close()
