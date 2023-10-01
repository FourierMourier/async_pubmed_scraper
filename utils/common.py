import yaml
from pathlib import Path
from typing import Union, Dict, Optional, Any


__all__ = ['load_yaml']


def load_yaml(filepath: Union[str, Path], **kwargs) -> Dict[str, Any]:
    with open(filepath, 'r', **kwargs) as file:
        config = yaml.safe_load(file)
    return config
