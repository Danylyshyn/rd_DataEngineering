import os
import logging
import requests
import threading
from lesson_02.job1 import local_disk


AUTH_TOKEN = os.environ.get("AUTH_TOKEN")

if not AUTH_TOKEN:
    logging.error("AUTH_TOKEN environment variable must be set")
    exit(1)

API_URL = os.getenv("API_URL", "https://fake-api-vycpfa6oca-uc.a.run.app/sales")


def save_json(filename, page, data) -> None:
    local_disk.save_to_disk(data, filename, page)


def save_sales_to_local_disk(date: str, raw_dir: str) -> int:

    logging.info(f"check path: {raw_dir}")
    raw_path = local_disk.check_path(raw_dir)

    logging.info(f"clean folder: {raw_dir}")
    local_disk.clean_folder(raw_path)

    page = 0
    while True:
        page += 1
        response = requests.get(
            url=API_URL,
            params={'date': date, 'page': page},
            headers={'Authorization': AUTH_TOKEN},
        )
        if response.status_code == 200:
            thread_file = threading.Thread(target=save_json(raw_path, page, response.json()))
            thread_file.start()
        else:
            break

    logging.info("\tI'm in get_sales(...) function!")
    return 200
