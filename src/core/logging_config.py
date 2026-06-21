"""Configuracion centralizada de logging usando loguru.

Salida dual: consola con colores y archivo con rotacion diaria (30 dias).
"""
import sys
from loguru import logger
from src.core.config import settings

def setup_logging():
    """Configura loguru para registrar en consola y archivo."""
    # Eliminar handlers existentes
    logger.remove()

    # Salida a consola
    logger.add(
        sys.stderr,
        level=settings.log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    )

    # Salida a archivo (rotacion diaria)
    logger.add(
        settings.logs_dir / "app_{time:YYYY-MM-DD}.log",
        rotation="1 day",
        retention="30 days",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    )
    
    return logger

# Inicializar una sola vez
logger = setup_logging()
