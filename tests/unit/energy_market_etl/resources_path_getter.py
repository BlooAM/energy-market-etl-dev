import os
from pathlib import Path


def get_resources_path() -> Path:
    current_path = Path(os.path.dirname(os.path.realpath(__file__)))
    return current_path / ".." / "resources"
