"""Scraper de la seccion /mis-reportes del portal del Observatorio.

Descarga sistematicamente archivos PDF/XLSX de reportes historicos.
"""
import os
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional
from playwright.async_api import Page, expect
from src.core.config import settings
from src.core.logging_config import logger
from src.core.utils import save_json, normalize_text

class ReportsScraper:
    """Navega /mis-reportes y descarga sistematicamente todos los archivos historicos."""
    
    def __init__(self, page: Page):
        self.page = page
        self.settings = settings
        self.reports_metadata: List[Dict[str, Any]] = []

    async def _crawl_pages(self) -> List[str]:
        """Navega las listas de reportes y recoge todos los enlaces especificos."""
        logger.info(f"Navegando al repositorio de reportes: {self.settings.obs_reports_url}")
        
        await self.page.goto(self.settings.obs_reports_url, timeout=self.settings.obs_timeout, wait_until="domcontentloaded")
        await self.page.wait_for_load_state("networkidle")
        
        # Extraer todos los enlaces de reportes
        report_links = await self.page.eval_on_selector_all(
            "a[href*='/mis-reportes/']", 
            "links => links.map(a => a.href)"
        )
        
        # Eliminar duplicados
        unique_links = list(set([l for l in report_links if '/mis-reportes/' in l and l != self.settings.obs_reports_url]))
        logger.info(f"Se encontraron {len(unique_links)} enlaces unicos de reportes.")
        return unique_links

    async def _download_files_from_report(self, report_url: str):
        """Visita una pagina de reporte y descarga todos los archivos asociados (PDF, XLSX)."""
        logger.debug(f"Explorando reporte: {report_url}")
        await self.page.goto(report_url, timeout=self.settings.obs_timeout, wait_until="domcontentloaded")
        await self.page.wait_for_load_state("domcontentloaded")
        
        # Extraer titulo y fecha
        try:
            title = await self.page.inner_text("h1")
            date_published = await self.page.inner_text(".fecha-publicacion") if await self.page.query_selector(".fecha-publicacion") else ""
        except Exception:
            title = f"Report_{report_url.split('/')[-1]}"
            date_published = ""

        # Identificar archivos para descarga
        download_selectors = await self.page.query_selector_all("a[href*='/descargar']")
        
        for selector in download_selectors:
            # Nombrar archivos descriptivamente: Ano_Mes_Titulo.ext
            filename = await selector.get_attribute("title") or await selector.inner_text()
            
            # Iniciar descarga
            try:
                async with self.page.expect_download() as download_info:
                    await selector.click()
                
                download_obj = await download_info.value
                ext = Path(download_obj.suggested_filename).suffix
                
                # Formato personalizado de nombre
                safe_name = "".join([c if c.isalnum() else "_" for c in normalize_text(title)])
                report_id = report_url.split("/")[-1]
                save_path = self.settings.raw_dir / f"report_{report_id}_{safe_name}{ext}"
                
                await download_obj.save_as(save_path)
                logger.info(f"Descargado: {save_path.name}")
                
                self.reports_metadata.append({
                    "title": title,
                    "date_published": date_published,
                    "url": report_url,
                    "file_path": str(save_path),
                    "original_filename": download_obj.suggested_filename
                })
            except Exception as e:
                logger.error(f"Fallo al descargar archivo desde {report_url}: {str(e)}")

    async def scrape_all_reports(self):
        """Crawl maestro de la seccion de reportes."""
        links = await self._crawl_pages()
        for i, link in enumerate(links):
            logger.info(f"Procesando reporte {i+1}/{len(links)}...")
            await self._download_files_from_report(link)
            # Espera corta para evitar rate limiting
            await asyncio.sleep(2)
        
        save_json(self.reports_metadata, self.settings.raw_dir / "reports_catalog.json")
        return self.reports_metadata
