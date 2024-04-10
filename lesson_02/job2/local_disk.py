import os
import glob
import json
import fastavro as avro
from pathlib import Path


schema = {
    'doc': 'JSON to AVRO schema file.',
    'name': 'JOB2',
    'namespace': 'lessons_02',
    'type': 'record',
    'fields': [
        {'name': 'client', 'type': 'string'},
        {'name': 'purchase_date', 'type': 'string'},
        {'name': 'product', 'type': 'string'},
        {'name': 'price', 'type': 'int'},
    ],
}


def convert_json_to_avro(raw_dir: str, stg_dir: str) -> None:

    raw_path = Path(raw_dir)
    if not raw_path.is_dir():
        print(f"{raw_dir} must be directory")
        exit(1)

    stg_path = Path(stg_dir)
    if not stg_path.is_dir():
        os.makedirs(stg_dir, exist_ok=True)

    raw_dir_files = os.path.join(raw_path, "*.json")
    for infile in glob.glob(raw_dir_files):
        file, ext = os.path.splitext(infile)
        filename = os.path.basename(file)
        # read only json files
        if ext == '.json':
            with open(infile, "r") as injson:
                data = json.load(injson)
                # data from json write to AVRO file with schema
                avro_file = os.path.join(stg_path, f"{filename}.avro")
                with open(avro_file, 'wb') as out:
                    avro.writer(out, schema, data)


if __name__ == '__main__':
    convert_json_to_avro("../raw/sales/2022-08-09", "../stg/sales/2022-08-09")
