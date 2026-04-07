import asyncio
from typing import List, Dict, Any, Optional
from playwright.async_api import Page, Response, Request
from src.core.config import settings
from src.core.logging_config import logger
from src.core.utils import save_json

class SiteDiscovery:
    """Explores the site to map all available modules, dimensions, and statistical paths."""
    
    def __init__(self, page: Page):
        self.page = page
        self.settings = settings
        self.inventory: List[Dict[str, Any]] = []

    async def discover_modules(self) -> List[Dict[str, Any]]:
        """Maps the main nav menu and sub-menus."""
        logger.info("Discovering site modules...")
        
        await self.page.goto(self.settings.obs_url, timeout=self.settings.obs_timeout)
        await self.page.wait_for_load_state("networkidle")
        
        # Check nav menu
        nav_selectors = await self.page.query_selector_all("nav a, .menu-item a")
        for nav in nav_selectors:
            name = await nav.inner_text()
            href = await nav.get_attribute("href")
            
            if href and "http" in href:
                 self.inventory.append({
                     "name": name.strip(),
                     "url": href,
                     "type": "navigation"
                 })
        
        # Identify statistical dashboards
        dashboard_paths = ["/alcalde", "/territorial", "/mapas", "/convergencia"]
        for p in dashboard_paths:
             full_url = f"{self.settings.obs_url}{p}"
             logger.info(f"Checking dashboard path: {full_url}")
             response = await self.page.goto(full_url)
             if response and response.status == 200:
                  self.inventory.append({
                      "name": f"Dashboard {p.replace('/', '').capitalize()}",
                      "url": full_url,
                      "type": "dashboard"
                  })
                  
        save_json(self.inventory, self.settings.raw_dir / "site_inventory.json")
        logger.info(f"Discovery complete. Found {len(self.inventory)} relevant paths.")
        return self.inventory
