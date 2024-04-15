import os
import glob
import json
import fastavro as avro
from fastavro.schema import load_schema_ordered
from pathlib import Path
import threading
import logging

schema_paths = ["parent.avsc"]
schema = load_schema_ordered(schema_paths)


def logging_decorator(func):
    def wrapper(*args, **kwargs):
        logging.info(f"Running {func.__name__} with arguments {args} and keyword arguments {kwargs}")
        return func(*args, **kwargs)
    return wrapper


@logging_decorator
def write_avro(src_file: str, stg_dir: str) -> None:
    file, ext = os.path.splitext(src_file)
    filename = os.path.basename(file)
    # read only json files
    if ext == '.json':
        with open(src_file, "r") as injson:
            data = json.load(injson)
            # data from json write to AVRO file with schema
            avro_file = os.path.join(stg_dir, f"{filename}.avro")
            with open(avro_file, 'wb') as out:
                avro.writer(out, schema, data)


def convert_json_to_avro(raw_dir: str, stg_dir: str) -> int:
    try:
        raw_path = Path(raw_dir)
        if not raw_path.is_dir():
            logging.error(f"{raw_dir} must be directory")
            return 1
    except OSError as ex:
        logging.exception(ex)
        return 1

    try:
        stg_path = Path(stg_dir)
        if not stg_path.is_dir():
            os.makedirs(stg_dir, exist_ok=True)
    except OSError as ex:
        logging.exception(ex)
        return 2

    raw_dir_files = os.path.join(raw_path, "*.json")
    for infile in glob.glob(raw_dir_files):
        thread_file = threading.Thread(target=write_avro(infile, stg_dir))
        thread_file.start()

