import unittest
import local_disk
import requests
from pathlib import Path
import os

FOLDER_PATH : str = '..\\raw\\sales\\2022-08-09'

AUTH_TOKEN = os.environ.get("AUTH_TOKEN")
API_URL = os.getenv("API_URL", "https://fake-api-vycpfa6oca-uc.a.run.app/sales")


class Job1TestCase(unittest.TestCase):
    def test_save_to_disk(self):
        file_data = [{"key1": "value1", "key2": "value2"}]
        self.assertEqual(local_disk.save_to_disk(file_data, Path(FOLDER_PATH),1), f"{FOLDER_PATH}\\sales_2022-08-09_1.json")  # add assertion here

    def test_check_api(self):
        if not AUTH_TOKEN:
            self.fail("AUTH_TOKEN")
        else:
            response = requests.get(
                url=API_URL,
                params={'date': '2022-08-09', 'page': 1},
                headers={'Authorization': AUTH_TOKEN},
            )
        self.assertEqual(response.status_code, 200)  # add assertion here


if __name__ == '__main__':
    unittest.main()
