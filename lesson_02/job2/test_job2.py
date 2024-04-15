import unittest
import local_disk
import os

RAW_DIR : str = '..\\raw\\sales\\2022-08-09'
STG_DIR : str = '..\\stg\\sales\\2022-08-09'

AUTH_TOKEN = os.environ.get("AUTH_TOKEN")
API_URL = os.getenv("API_URL", "https://fake-api-vycpfa6oca-uc.a.run.app/sales")


class Job2TestCase(unittest.TestCase):
    def test_convert_json_to_avro(self):
        self.assertEqual(local_disk.convert_json_to_avro(RAW_DIR, STG_DIR), None)  # add assertion here


if __name__ == '__main__':
    unittest.main()
