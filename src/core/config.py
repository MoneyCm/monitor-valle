import os
from pathlib import Path
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # Credentials
    obs_user: Optional[str] = Field(None, alias="OBS_USER")
    obs_password: Optional[str] = Field(None, alias="OBS_PASSWORD")

    # Platform Settings
    obs_url: str = "https://www.observatoriodeldelitovalle.co"
    obs_login_url: str = "https://www.observatoriodeldelitovalle.co/login"
    obs_alcalde_url: str = "https://www.observatoriodeldelitovalle.co/alcalde"
    obs_reports_url: str = "https://www.observatoriodeldelitovalle.co/mis-reportes"
    
    # Extraction Settings
    obs_municipio: str = "Jamundí"
    obs_timeout: int = 60000  # ms
    obs_headless: bool = True
    
    # Paths
    base_dir: Path = Path(__file__).resolve().parent.parent.parent
    data_dir: Path = base_dir / "data"
    raw_dir: Path = data_dir / "raw"
    processed_dir: Path = data_dir / "processed"
    final_dir: Path = data_dir / "final"
    logs_dir: Path = base_dir / "logs"

    # Log Settings
    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    def create_dirs(self):
        """Create necessary directories if they don't exist."""
        for d in [self.data_dir, self.raw_dir, self.processed_dir, self.final_dir, self.logs_dir]:
            d.mkdir(parents=True, exist_ok=True)

settings = Settings()
settings.create_dirs()
