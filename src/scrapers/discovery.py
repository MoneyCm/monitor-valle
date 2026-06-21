"""Modulo de descubrimiento del sitio del Observatorio.

Mapea modulos, dimensiones y rutas estadisticas disponibles.
"""
import asyncio
from typing import List, Dict, Any, Optional
from playwright.async_api import Page, Response, Request
from src.core.config import settings
from src.core.logging_config import logger
from src.core.utils import save_json

class SiteDiscovery:
    """Explora el sitio para mapear modulos, dimensiones y rutas estadisticas disponibles."""
    
    def __init__(self, page: Page):
        self.page = page
        self.settings = settings
        self.inventory: List[Dict[str, Any]] = []

    async def discover_modules(self) -> List[Dict[str, Any]]:
        """Mapea el menu de navegacion principal y submenus."""
        logger.info("Descubriendo modulos del sitio...")
        
        await self.page.goto(self.settings.obs_url, timeout=self.settings.obs_timeout)
        await self.page.wait_for_load_state("networkidle")
        
        # Explorar menu de navegacion
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
        
        # Identificar dashboards estadisticos
        dashboard_paths = ["/alcalde", "/territorial", "/mapas", "/convergencia"]
        for p in dashboard_paths:
             full_url = f"{self.settings.obs_url}{p}"
             logger.info(f"Verificando ruta de dashboard: {full_url}")
             response = await self.page.goto(full_url)
             if response and response.status == 200:
                  self.inventory.append({
                      "name": f"Dashboard {p.replace('/', '').capitalize()}",
                      "url": full_url,
                      "type": "dashboard"
                  })
                  
        save_json(self.inventory, self.settings.raw_dir / "site_inventory.json")
        logger.info(f"Descubrimiento completado. Se encontraron {len(self.inventory)} rutas relevantes.")
        return self.inventory
