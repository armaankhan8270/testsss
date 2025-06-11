"""
Utility Modules for Data Extraction System
Contains supporting components for configuration, logging, file management, etc.
"""

# =============================================================================
# utils/config_manager.py
# =============================================================================

import os
import yaml
import logging
from pathlib import Path
from typing import Dict, Any


class ConfigManager:
    """Manages configuration files for the extraction system."""
    
    def __init__(self, config_dir: Path):
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(parents=True, exist_ok=True)
    
    def save_config(self, config: Dict[str, Any], project_name: str) -> None:
        """Save configuration to YAML file."""
        
        config_path = self.config_dir / f"{project_name}.yaml"
        
        try:
            with open(config_path, "w