import os
import requests
from lesson_02.job1 import local_disk


AUTH_TOKEN = os.environ.get("AUTH_TOKEN")

if not AUTH_TOKEN:
    print("AUTH_TOKEN environment variable must be set")
    exit(1)


def save_sales_to_local_disk(date: str, raw_dir: str) -> None:

    raw_path = local_disk.check_path(raw_dir)

    local_disk.clean_folder(raw_path)

    page = 1
    while True:
        response = requests.get(
            url='https://fake-api-vycpfa6oca-uc.a.run.app/sales',
            params={'date': date, 'page': page},
            headers={'Authorization': AUTH_TOKEN},
        )
        if response.status_code == 200:
            local_disk.save_to_disk(response.json(), raw_path, page)
            page = page + 1
        else:
            break

    print("\tI'm in get_sales(...) function!")
    pass
