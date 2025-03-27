import yaml
from typing import Union
import os

# ==== Customized Modules ====
from .paths import PROJECT_PATH


class ConfigLoader:
    @staticmethod
    def load_config(
        config_root_path: str = PROJECT_PATH, config_path: Union[str, None] = None
    ) -> dict:
        if not config_path:
            config_path = os.path.join(config_root_path, "config.yaml")
        return yaml.load(open(config_path, "r"), Loader=yaml.FullLoader)
