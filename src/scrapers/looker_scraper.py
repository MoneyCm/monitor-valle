"""Scraper de datos de Looker Studio embebido en el portal del Observatorio.

Extrae datos analiticos mediante dos estrategias paralelas:
1. Interceptacion de respuestas XHR (JSON interno de Looker)
2. Exportacion CSV via UI (3 puntos -> Exportar -> CSV)
"""
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
    """Extrae datos de Looker Studio usando exportacion directa e interceptacion XHR."""
    
    def __init__(self, page: Page):
        self.page = page
        self.settings = settings
        self.municipio = settings.obs_municipio
        self.municipio_prefix = self.municipio.lower()[:6]  # "jamund" para busqueda UI
        self.municipio_slug = settings.municipio_slug
        self.captured_requests: List[Dict[str, Any]] = []
        self.captured_responses: List[Dict[str, Any]] = [] 
        self.exported_files: List[Path] = []
        self.current_year_focus = "Initial"

    async def _handle_request(self, request: Request):
        """Intercepta todas las solicitudes para discovery/tracing."""
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
        """Intercepta respuestas XHR que contienen los bloques de datos reales."""
        is_looker_data = "google" in response.url and ("get_data" in response.url or "batchedData" in response.url)
        
        if is_looker_data:
            try:
                if response.status == 200:
                    text = await response.text()
                    # Google a menudo prefija JSON con )]}' para prevenir XSSI
                    if text.startswith(")]}'"):
                        text = text[4:].strip()
                    
                    payload = json.loads(text)
                    self.captured_responses.append({
                        "url": response.url,
                        "data": payload,
                        "year_tag": self.current_year_focus
                    })
                    logger.success(f"Bloque de datos capturado ({len(text)} bytes) de {response.url[:50]}... (Total: {len(self.captured_responses)})")
            except Exception as e:
                logger.warning(f"No se pudo parsear respuesta de {response.url[:30]}: {str(e)}")

    async def trigger_export(self) -> Optional[Path]:
        """Automatiza el flujo '3 puntos -> Exportar -> CSV' usando Frame Locator."""
        logger.info("Intentando exportar CSV desde la UI de Looker Studio...")
        
        looker_frame = self.page.frame_locator('iframe[src*="lookerstudio.google.com"]')
        
        try:
            # Apuntar al heading de la tabla para identificar el contenedor
            logger.debug("Esperando heading especifico de tabla...")
            table_heading = looker_frame.get_by_role("heading", name="Casos por Ano y Mes").first
            await table_heading.wait_for(state="visible", timeout=45000)
            
            await table_heading.hover()
            await asyncio.sleep(4)
            
            # Boton 'Mas Opciones' (3 puntos)
            menu_btn = looker_frame.locator('button[aria-label*="opciones"], button[aria-label*="options"], .flyout-menu-button, button:has(i:has-text("more_vert")), button:has(span:has-text("more_vert"))').first
            await menu_btn.wait_for(state="visible", timeout=20000)
            await menu_btn.click(timeout=20000)
            logger.debug("Menu contextual de tabla abierto.")
            await asyncio.sleep(2)
            
            # Seleccionar 'Exportar' del dropdown
            export_item = looker_frame.locator('text="Exportar", text="Export", .mat-menu-item').first
            await export_item.wait_for(state="visible", timeout=10000)
            await export_item.click(timeout=10000)
            logger.debug("Dialogo de exportacion activado.")
            await asyncio.sleep(4)
            
            # Confirmacion final en el dialogo de Looker
            async with self.page.expect_download(timeout=60000) as download_info:
                await looker_frame.locator('button:has-text("EXPORTAR"), button:has-text("EXPORT")').last.click(timeout=10000)
            
            download = await download_info.value
            save_path = self.settings.raw_dir / f"looker_export_{self.municipio_slug}_{download.suggested_filename}"
            await download.save_as(save_path)
            
            logger.success(f"Datos de Looker exportados exitosamente a {save_path}")
            self.exported_files.append(save_path)
            return save_path
            
        except Exception as e:
            logger.warning(f"La interaccion de exportacion UI fallo: {str(e)}.")
            await self.page.screenshot(path=str(self.settings.logs_dir / "export_failure_report.png"))
            return None

    async def extract_dashboard_data(self) -> Dict[str, Any]:
        """Punto de entrada principal para la extraccion de datos de Looker."""
        # 0. Iniciar desde Home para cumplir requisito de "click en boton"
        home_url = f"{self.settings.obs_url}/home"
        logger.info(f"Navegando a Home para iniciar via boton 'Informe Alcaldes': {home_url}")
        
        self.page.on("request", self._handle_request)
        self.page.on("response", self._handle_response)
        
        await self.page.goto(home_url, timeout=self.settings.obs_timeout, wait_until="domcontentloaded")
        await self.page.wait_for_load_state("networkidle")
        
        # 1. Click en 'Informe Alcaldes'
        try:
            logger.info("Click en boton 'Informe Alcaldes'...")
            btn = self.page.get_by_role("link", name="Informe Alcaldes").first
            await btn.click(timeout=10000)
            await self.page.wait_for_load_state("networkidle")
            logger.success(f"Alcanzado dashboard: {self.page.url}")
        except Exception as e:
            logger.warning(f"Fallo al click en boton, navegando directamente a {self.settings.obs_alcalde_url}. Error: {e}")
            await self.page.goto(self.settings.obs_alcalde_url, timeout=self.settings.obs_timeout, wait_until="domcontentloaded")

        # 2. Esperar inicializacion de Looker Studio
        try:
            await self.page.wait_for_load_state("load", timeout=60000)
            looker_frame = self.page.frame_locator('iframe[src*="lookerstudio.google.com"]')
            # Esperar a que los controles del iframe esten visibles
            await looker_frame.locator('.lego-control').first.wait_for(state="visible", timeout=30000)
            logger.info("Looker Studio inicializado correctamente.")
            await asyncio.sleep(5)
        except Exception:
            logger.warning("Timeout esperando carga completa de Looker, procediendo con mejor esfuerzo.")
            await asyncio.sleep(5)

        looker_frame = self.page.frame_locator('iframe[src*="lookerstudio.google.com"]')

        # FASE 2: Asegurar filtro de municipio
        logger.info(f"Asegurando filtro de municipio para {self.municipio}...")
        try:
            # 1. Abrir el filtro
            municipio_trigger = looker_frame.locator('div:has-text("MUNICIPIO"), div:has-text("Municipio"), .lego-control').filter(has_text=re.compile(r"MUNICIPIO|Municipio", re.I)).last
            
            await municipio_trigger.hover()
            await asyncio.sleep(1)
            await municipio_trigger.click(force=True)
            await asyncio.sleep(3)
            
            # 2. Buscar municipio en la caja de busqueda interna
            search_box = looker_frame.locator('input[placeholder*="Buscar"], input[placeholder*="Search"], input[type="text"]').first
            if await search_box.is_visible(timeout=5000):
                await search_box.fill(self.municipio_prefix)
                await asyncio.sleep(2)

            # 3. Usar 'Solamente' (Solo) para seleccionar solo el municipio
            municipio_option = looker_frame.get_by_text(self.municipio_prefix.capitalize(), exact=False).first
            
            # Si el localizador por texto no es suficiente, intentar combinado
            if not await municipio_option.is_visible(timeout=5000):
                 municipio_option = looker_frame.locator('.ng2-menu-item, .mat-menu-item, div[role="option"]').filter(has_text=re.compile(rf"{re.escape(self.municipio_prefix)}.*", re.I)).first
            
            if await municipio_option.is_visible(timeout=5000):
                logger.info(f"Opcion de {self.municipio} encontrada, intentando click en 'Solamente'...")
                await municipio_option.hover()
                await asyncio.sleep(1)

                # Buscar link 'Solamente' que aparece al pasar el mouse
                only_button = looker_frame.get_by_text("Solamente", exact=False).first
                if not await only_button.is_visible(timeout=2000):
                     only_button = municipio_option.locator('span', has_text=re.compile(r"Solamente|Only|Solo", re.IGNORECASE)).first
                
                if await only_button.is_visible(timeout=3000):
                    logger.info("Click en 'Solamente' para filtrar estrictamente.")
                    await only_button.click(force=True)
                    # Limpiar bloques capturados ANTES de este clic (datos globales del Valle)
                    await asyncio.sleep(5)
                    logger.info("LIMPIEZA: Descartando bloques de datos globales previos...")
                    self.captured_responses.clear()
                else:
                    logger.info(f"Seleccionando opcion de {self.municipio} directamente (check/toggle).")
                    await municipio_option.click(force=True)
                    await asyncio.sleep(5)
                    self.captured_responses.clear()
                
                logger.success(f"Municipio {self.municipio} seleccionado.")
            else:
                logger.warning(f"No se encontro la opcion de {self.municipio} en la lista de filtros.")

        except Exception as e:
            logger.warning(f"Problema con la seleccion de filtro de municipio: {str(e)}")
        finally:
            await self.page.keyboard.press("Escape")
            await asyncio.sleep(2)

        # Esperar a que se capturen todos los bloques filtrados.
        # IMPORTANTE: No limpiar captured_responses aqui; el parser se encarga de filtrar.
        # Looker Studio necesita tiempo (~30s) para recargar los datos tras aplicar el filtro.
        logger.info("Esperando 30s para asegurar captura de todos los bloques filtrados...")
        await asyncio.sleep(30)
        
        # 4. Seleccion secuencial de anos para desgloses detallados
        years_to_capture = ["2024", "2025", "2026"]
        logger.info(f"Iniciando extraccion ano por ano para {years_to_capture}...")
        
        for year in years_to_capture:
            try:
                self.current_year_focus = year 
                logger.info(f"Filtrando por Ano: {year}...")
                
                anio_trigger = looker_frame.locator('button.lego-control').filter(has_text=re.compile(r"ANO|Ano", re.I)).first
                
                if await anio_trigger.is_visible(timeout=10000):
                    await anio_trigger.hover()
                    await asyncio.sleep(1)
                    await anio_trigger.click(force=True, timeout=10000)
                else:
                    logger.warning(f"Boton 'ANO' no visible directamente para ano {year}, intentando click generico...")
                    target = looker_frame.get_by_text("ANO", exact=False).first
                    await target.hover()
                    await target.click(force=True, timeout=10000)
                
                await asyncio.sleep(4)
                
                logger.info(f"Aplicando filtro robusto para ano: {year}")
                
                # Click JS para evitar overlays
                await anio_trigger.evaluate("el => el.dispatchEvent(new MouseEvent('click', {bubbles: true}))")
                await asyncio.sleep(3)
                
                # Buscar ano
                search_box_year = looker_frame.locator('input[placeholder*="Buscar"], input[placeholder*="Search"], input[type="text"]').first
                if await search_box_year.is_visible(timeout=5000):
                    await search_box_year.fill(year)
                    await asyncio.sleep(2)
                
                # Encontrar opcion y click en 'Solamente' via JS
                year_option = looker_frame.get_by_text(year, exact=True).first
                if await year_option.is_visible(timeout=5000):
                    logger.info(f"Opcion {year} encontrada, activando Solamente via JS...")
                    
                    only_btn_year = looker_frame.get_by_text("Solamente", exact=False).first
                    if await only_btn_year.is_visible(timeout=3000):
                         await only_btn_year.evaluate("el => el.dispatchEvent(new MouseEvent('click', {bubbles: true}))")
                    else:
                         await year_option.evaluate("el => el.dispatchEvent(new MouseEvent('click', {bubbles: true}))")
                    
                    await asyncio.sleep(3)
                    await self.page.keyboard.press("Escape")
                else:
                    logger.warning(f"Opcion de ano {year} no encontrada en dropdown")
                
                logger.info(f"Esperando 25s para refresh de datos de {year}...")
                await asyncio.sleep(25)
                
                logger.success(f"Secuencia de datos capturada para {year}")
                await self.page.keyboard.press("Escape")
                await asyncio.sleep(2)
            except Exception as e:
                logger.warning(f"Fallo al filtrar ano {year}: {str(e)}")
                await self.page.screenshot(path=str(self.settings.logs_dir / f"failure_year_{year}.png"))

        # Guardar respuestas crudas colectivas
        save_json(self.captured_responses, self.settings.raw_dir / "captured_responses.json")
        
        # Intentar exportacion directa (respaldo)
        csv_path = await self.trigger_export()
        
        return {
            "csv_path": str(csv_path) if csv_path else None,
            "requests_count": len(self.captured_requests),
            "responses_count": len(self.captured_responses)
        }
