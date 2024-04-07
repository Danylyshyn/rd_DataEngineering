import os
import glob
import json
import fastavro as avro
from pathlib import Path


def convert_json_to_avro(raw_dir: str, stg_dir: str) -> None:
    raw_dir_universal = Path(raw_dir)

    if not raw_dir_universal.is_dir():
        print(f"{raw_dir} must be directory")
        exit(1)
    stg_dir_universal = Path(stg_dir)
    if not stg_dir_universal.is_dir():
        os.makedirs(stg_dir, exist_ok=True)

    schema = {
        'doc': 'JSON to AVRO schema file.',
        'name': 'JOB2',
        'namespace': 'lessons_02',
        'type': 'record',
        'fields': [
            {'name': 'client', 'type': 'string'},
            {'name': 'purchase_date', 'type': 'string'},
            {'name': 'product', 'type': 'string'},
            {'name': 'price', 'type': 'long'},
        ],
    }

    _fullpath = ""
    for lPath in stg_dir_universal.parts:
        _fullpath = os.path.join(_fullpath, lPath)
        if not os.path.exists(_fullpath) and not stg_dir_universal == _fullpath:
            os.mkdir(_fullpath)

    raw_dir_files = os.path.join(raw_dir_universal, "*")
    for infile in glob.glob(raw_dir_files):
        file, ext = os.path.splitext(infile)
        filename = os.path.basename(file)
        # print(f'Processing {file} {ext}')
        # read only json files
        if ext == '.json':
            with open(infile, "r") as injson:
                data = json.load(injson)
                # data from json write to AVRO file with schema
                avro_file = os.path.join(stg_dir_universal, f"{filename}.avro")
                with open(avro_file, 'wb') as out:
                    avro.writer(out, schema, data)

# convert_json_to_avro("D:\\LABA\\RD\\src\\rd_DataEngineering\\lesson_02\\raw\\sales\\2022-08-09", "D:\\LABA\\RD\\src\\rd_DataEngineering\\lesson_02\\stg\\sales\\2022-08-09")
