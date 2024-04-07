from lesson_02.job1 import local_disk
import os
import requests


AUTH_TOKEN = os.environ.get("AUTH_TOKEN")

if not AUTH_TOKEN:
    print("AUTH_TOKEN environment variable must be set")
    exit(1)


def save_sales_to_local_disk(date: str, raw_dir: str) -> None:
    response = requests.get(
        url='https://fake-api-vycpfa6oca-uc.a.run.app/sales',
        params={'date': date, 'page': 2},
        headers={'Authorization': AUTH_TOKEN},
    )

    local_disk.save_to_disk(response.json(), raw_dir)

    print("\tI'm in get_sales(...) function!")
    pass
