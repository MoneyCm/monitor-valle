import pytest
from src.core.config import settings

def test_config_paths():
    """Verify that configuration paths are correctly initialized."""
    assert settings.base_dir.exists()
    assert settings.data_dir.name == "data"
    assert settings.raw_dir.name == "raw"

def test_env_loading():
    """Check if environment variables can be accessed (requires .env or mock)."""
    # This might fail if no .env exists, which is fine for a template test
    assert hasattr(settings, 'obs_user')
    assert hasattr(settings, 'obs_password')
