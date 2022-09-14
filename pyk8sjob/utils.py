from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, List

import yaml
from pyk8sjob.config import config


@dataclass
class Preset:
    image: Optional[str] = None
    ttl_seconds_after_finished: Optional[int] = None
    env: Optional[Dict[str, str]] = None
    parallelism: Optional[int] = None
    node_selector: Optional[Dict[str, str]] = None
    resources: Optional[Dict[str, Dict[str, str]]] = None
    tolerations: Optional[List[Dict[str, str]]] = None


def get_preset(name: str) -> Preset:
    path = Path(config.presets_path)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(exist_ok=True)
    with path.open() as f:
        presets = yaml.safe_load(f)
    return Preset(**presets[name])


def parse_env(envs: List[str]):
    env_vars = {}
    for env in envs:
        k, v = env.split("=")
        env_vars[k] = v
    return env_vars
