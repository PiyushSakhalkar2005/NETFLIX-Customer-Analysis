import os
import re
import yaml
from typing import Any, Dict

class ConfigLoader:
    _config: Dict[str, Any] = None

    @classmethod
    def load(cls, config_path: str = None) -> Dict[str, Any]:
        if config_path is None and cls._config is not None:
            return cls._config
        
        if config_path is None:
            # Try default path relative to script
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            config_path = os.path.join(base_dir, "config", "pipeline_config.yaml")

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found at: {config_path}")

        with open(config_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Parse YAML
        config = yaml.safe_load(content)

        # Resolve base_path relative to project root if relative or invalid
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        if "storage" in config and "base_path" in config["storage"]:
            base_path = config["storage"]["base_path"]
            if not os.path.isabs(base_path) or not os.path.exists(base_path):
                resolved_path = os.path.abspath(os.path.join(project_root, base_path.lstrip("/\\")))
                if not os.path.exists(resolved_path) and os.path.exists(os.path.join(project_root, "data")):
                    resolved_path = os.path.join(project_root, "data")
                config["storage"]["base_path"] = resolved_path.replace("\\", "/")

        # Resolve variables like ${storage.base_path}
        resolved = cls._resolve_variables(config)
        if config_path is None:
            cls._config = resolved
        return resolved

    @classmethod
    def _resolve_variables(cls, config: Any) -> Any:
        def resolve_val(val: str, root_cfg: dict) -> str:
            matches = re.findall(r"\$\{([^}]+)\}", val)
            for match in matches:
                parts = match.split(".")
                curr = root_cfg
                for p in parts:
                    if isinstance(curr, dict) and p in curr:
                        curr = curr[p]
                    else:
                        curr = None
                        break
                if curr is not None:
                    if isinstance(curr, str) and "${" in curr:
                        curr = resolve_val(curr, root_cfg)
                    val = val.replace(f"${{{match}}}", str(curr))
            return val

        def traverse_and_resolve(node: Any, root_cfg: dict) -> Any:
            if isinstance(node, dict):
                return {k: traverse_and_resolve(v, root_cfg) for k, v in node.items()}
            elif isinstance(node, list):
                return [traverse_and_resolve(item, root_cfg) for item in node]
            elif isinstance(node, str):
                return resolve_val(node, root_cfg)
            return node

        return traverse_and_resolve(config, config)
