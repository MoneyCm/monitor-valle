import os
import asyncio
from typing import Optional
from playwright.async_api import async_playwright, Browser, BrowserContext, Page, expect
from src.core.config import settings
from src.core.logging_config import logger
from tenacity import retry, wait_exponential, stop_after_attempt

class AuthManager:
    """Manages login and authentication state for the Observatorio portal."""
    
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
        """Performs login and returns a page instance with the active session."""
        
        # If no playwright instance provided, create one (this is usually handled by a context manager)
        if playwright_manager is None:
            self._pw = await async_playwright().start()
            playwright_manager = self._pw
            
        self.browser = await playwright_manager.chromium.launch(
            headless=self.settings.obs_headless,
            # Increase slow_mo if the site is sensitive
        )
        self.context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        self.page = await self.context.new_page()
        
        logger.info(f"Attempting login to {self.settings.obs_login_url}")
        await self.page.goto(self.settings.obs_login_url, timeout=self.settings.obs_timeout)
        
        # Fill credentials
        await self.page.fill("#email", self.settings.obs_user)
        await self.page.fill("#password", self.settings.obs_password)
        
        # Click login button
        await self.page.click(".btn-enviar-login")
        
        # Check for success (navigation or dashboard visibility)
        try:
            # Wait for either navigation or a dashboard element
            await self.page.wait_for_load_state("networkidle")
            # If redirected to a dashboard, success.
            if "/login" not in self.page.url:
                 logger.info(f"Login successful! Redirected to: {self.page.url}")
                 return self.page
            else:
                logger.error("Still on login page. Checking for error messages...")
                content = await self.page.content()
                if "credenciales" in content.lower():
                     raise Exception("Invalid credentials.")
                raise Exception("Login failed. Navigation did not occur.")
        except Exception as e:
            # Take screenshot for debugging if it fails
            await self.page.screenshot(path=str(self.settings.logs_dir / f"login_error_{os.getpid()}.png"))
            logger.error(f"Login error detail: {str(e)}")
            raise

    async def get_auth_headers(self) -> dict:
        """Extracts cookies/tokens to potentially use with HTTPX for direct API consumption."""
        if not self.context:
            return {}
        cookies = await self.context.cookies()
        # Transform cookies into a header-ready format if needed
        cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
        return {"Cookie": cookie_str}

    async def close(self):
        """Clean up browser resources."""
        if self.browser:
            await self.browser.close()
        if hasattr(self, '_pw') and self._pw:
            await self._pw.stop()
