import os
from pydantic import BaseSettings


class Config(BaseSettings):
    cluster_name: str = None
    default_dkr_image: str = "python:3.7.13-slim-buster"
    presets_path: str = os.path.expanduser("~/.config/pyk8sjob/presets.yaml")

    class Config:
        env_prefix = "PYK8SJOB_"


config = Config()
