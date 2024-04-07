import json
import os
from pathlib import Path
from typing import List, Dict, Any


def save_to_disk(json_content: List[Dict[str, Any]], path: str) -> None:
    path_universal = Path(path)
    os.makedirs(path, exist_ok=True)

    _filepath = os.path.join(path_universal,f'sales_{path_universal.parts[len(path_universal.parts)-1]}.json')
    if os.path.exists(_filepath):
        os.remove(_filepath)
    with open(_filepath, 'x') as f:
         json.dump(json_content,f)
    pass
