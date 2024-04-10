import json
import os
from pathlib import Path
from typing import List, Dict, Any

def clean_folder(folder_path: Path) -> None:
    for f in folder_path.iterdir():
        os.remove(str(f))


def check_path(folder_path: str) -> Path:
    raw_dir = Path(folder_path)
    os.makedirs(raw_dir, exist_ok=True)
    return raw_dir


def save_to_disk(json_content: List[Dict[str, Any]], path: Path, page: int) -> None:
    filename = path.parts[len(path.parts)-1]
    _filepath = os.path.join(path,f'sales_{filename}_{page}.json')
    if os.path.exists(_filepath):
        os.remove(_filepath)
    with open(_filepath, 'x') as f:
        json.dump(json_content,f)

