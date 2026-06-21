"""Gestor de autenticacion para el portal del Observatorio del Delito Valle.

Maneja login, sesion y cookies usando Playwright con reintentos automaticos.
"""
import os
import asyncio
from typing import Optional
from playwright.async_api import async_playwright, Browser, BrowserContext, Page, expect
from src.core.config import settings
from src.core.logging_config import logger
from tenacity import retry, wait_exponential, stop_after_attempt

class AuthManager:
    """Gestiona login y estado de autenticacion para el portal del Observatorio."""
    
    def __init__(self):
        self.settings = settings
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.browser: Optional[Browser] = None

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        reraise=True
    )
    async def login(self, playwright_manager=None) -> Page:
        """Realiza el login y retorna una instancia de pagina con la sesion activa."""
        
        # Si no se proporciona instancia de playwright, crear una
        if playwright_manager is None:
            self._pw = await async_playwright().start()
            playwright_manager = self._pw
            
        self.browser = await playwright_manager.chromium.launch(
            headless=self.settings.obs_headless,
        )
        self.context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        self.page = await self.context.new_page()
        
        logger.info(f"Intentando login en {self.settings.obs_login_url}")
        await self.page.goto(self.settings.obs_login_url, timeout=self.settings.obs_timeout, wait_until="domcontentloaded")
        # Completar credenciales
        await self.page.fill("#email", self.settings.obs_user)
        await self.page.fill("#password", self.settings.obs_password)
        
        # Click en boton de login
        await self.page.click(".btn-enviar-login")
        
        # Verificar exito (navegacion o visibilidad del dashboard)
        try:
            await self.page.wait_for_load_state("networkidle")
            if "/login" not in self.page.url:
                 logger.info(f"Login exitoso! Redirigido a: {self.page.url}")
                 return self.page
            else:
                logger.error("Aun en pagina de login. Verificando mensajes de error...")
                content = await self.page.content()
                if "credenciales" in content.lower():
                     raise Exception("Credenciales invalidas.")
                raise Exception("Login fallido. No se produjo la navegacion esperada.")
        except Exception as e:
            # Captura de pantalla para depuracion en caso de fallo
            await self.page.screenshot(path=str(self.settings.logs_dir / f"login_error_{os.getpid()}.png"))
            logger.error(f"Detalle del error de login: {str(e)}")
            raise

    async def get_auth_headers(self) -> dict:
        """Extrae cookies/tokens para uso potencial con httpx en consumo directo de API."""
        if not self.context:
            return {}
        cookies = await self.context.cookies()
        cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
        return {"Cookie": cookie_str}

    async def close(self):
        """Libera recursos del navegador."""
        if self.browser:
            await self.browser.close()
        if hasattr(self, '_pw') and self._pw:
            await self._pw.stop()
