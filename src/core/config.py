"""Configuracion centralizada del proyecto usando pydantic-settings.

Lee credenciales y parametros del archivo .env o variables de entorno.
"""
import os
from pathlib import Path
from typing import Optional
from unicodedata import normalize
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _normalize_slug(text: str) -> str:
    """Normaliza un texto a slug minusculo sin acentos ni espacios.

    Ejemplo: 'Jamundi' -> 'jamundi'
    """
    # NFD descompone: 'í' -> 'i' + combinacion de acento
    nfkd = normalize('NFD', text)
    no_accents = ''.join(c for c in nfkd if not (len(c) > 1 or ord(c) > 127 and not c.isascii()) or c.isascii() and not (0x0300 <= ord(c) <= 0x036F))
    return ''.join(c for c in nfkd if c.isascii() and not (0x0300 <= ord(c) <= 0x036F)).lower().replace(" ", "_").replace("-", "_")


class Settings(BaseSettings):
    # Credenciales
    obs_user: Optional[str] = Field(None, alias="OBS_USER")
    obs_password: Optional[str] = Field(None, alias="OBS_PASSWORD")

    # Plataforma
    obs_url: str = "https://www.observatoriodeldelitovalle.co"
    obs_login_url: str = "https://www.observatoriodeldelitovalle.co/login"
    obs_alcalde_url: str = "https://www.observatoriodeldelitovalle.co/alcalde"
    obs_reports_url: str = "https://www.observatoriodeldelitovalle.co/mis-reportes"
    
    # Extraccion
    obs_municipio: str = "Jamundi"
    obs_timeout: int = 60000  # ms
    obs_headless: bool = True
    
    # Rutas
    base_dir: Path = Path(__file__).resolve().parent.parent.parent
    data_dir: Path = base_dir / "data"
    raw_dir: Path = data_dir / "raw"
    processed_dir: Path = data_dir / "processed"
    final_dir: Path = data_dir / "final"
    logs_dir: Path = base_dir / "logs"

    # Logs
    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    @property
    def municipio_slug(self) -> str:
        """Slug normalizado del municipio (ej: 'jamundi')."""
        return _normalize_slug(self.obs_municipio)

    def create_dirs(self):
        """Crea los directorios necesarios si no existen."""
        for d in [self.data_dir, self.raw_dir, self.processed_dir, self.final_dir, self.logs_dir]:
            d.mkdir(parents=True, exist_ok=True)


settings = Settings()
settings.create_dirs()
