from pathlib import Path
from typing import Any, Dict, Optional

import yaml

DEFAULT_CONFIG: Dict[str, Dict[str, Any]] = {
    "storage": {
        "type": "local",
        "compression_level": 3,
        "chunk_size": 1048576,  # 1 MB
    },
    "database": {"type": "duckdb", "path": ".frostbyte/manifest.db"},
    "versioning": {"auto_version": True, "keep_all_versions": True},
}


class Config:
    def __init__(self, config_path: Optional[str] = None):
        if config_path:
            self.config_path = Path(config_path)
        else:
            self.config_path = Path(".frostbyte") / "config.yaml"

        self.config: Dict[str, Dict[str, Any]] = DEFAULT_CONFIG.copy()
        self._load()

    def _load(self) -> None:
        if self.config_path.exists():
            try:
                with open(self.config_path) as f:
                    user_config = yaml.safe_load(f)

                if user_config:
                    # Update the default config with user settings
                    # This is a simple update, for nested dicts consider deep merge
                    for section, values in user_config.items():
                        if section in self.config:
                            self.config[section].update(values)
                        else:
                            self.config[section] = values
            except Exception as e:
                print(f"Warning: Failed to load config from {self.config_path}: {e}")

    def save(self) -> None:
        self.config_path.parent.mkdir(exist_ok=True)

        with open(self.config_path, "w") as f:
            yaml.dump(self.config, f, default_flow_style=False)

    def get(self, section: str, key: str, default: Any = None) -> Any:
        return self.config.get(section, {}).get(key, default)

    def set(self, section: str, key: str, value: Any) -> None:
        """
        Set a configuration value.

        Args:
            section: Configuration section
            key: Configuration key
            value: Value to set
        """
        if section not in self.config:
            self.config[section] = {}

        self.config[section][key] = value

    def get_compression_level(self) -> int:
        """Get the compression level."""
        return int(self.get("storage", "compression_level", 3))

    def get_chunk_size(self) -> int:
        """Get the chunk size for reading/writing files."""
        return int(self.get("storage", "chunk_size", 1048576))

    def get_database_type(self) -> str:
        """Get the database type."""
        return str(self.get("database", "type", "duckdb"))

    def get_database_path(self) -> str:
        """Get the database path."""
        return str(self.get("database", "path", ".frostbyte/manifest.db"))
