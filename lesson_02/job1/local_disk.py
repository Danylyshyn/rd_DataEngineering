import json
import logging
import os
from pathlib import Path
from typing import List, Dict, Any


def check_path(folder_path: str) -> Path:
    raw_dir = Path(folder_path)
    try:
        os.makedirs(raw_dir, exist_ok=True)
    except OSError as ex:
        logging.exception(ex)
        return Path('')
    finally:
        return raw_dir


def save_to_disk(json_content: List[Dict[str, Any]], path: Path, page: int) -> str:
    filename = path.parts[len(path.parts) - 1]
    filepath = os.path.join(path, f'sales_{filename}_{page}.json')
    if os.path.exists(filepath):
        os.remove(filepath)
    with open(filepath, 'x') as f:
        json.dump(json_content, f)
    return filepath
