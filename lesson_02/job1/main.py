"""
This file contains the controller that accepts command via HTTP
and trigger business logic layer
"""
import os
import logging
from flask import Flask, request
from flask import typing as flask_typing
from lesson_02.job1.sales_api import save_sales_to_local_disk


SERVER_HOST = os.getenv("SERVER_HOST", "localhost")
SERVER_PORT = os.getenv("SERVER_PORT", 8086)


app = Flask(__name__)


@app.route('/', methods=['POST'])
def main() -> flask_typing.ResponseReturnValue:
    """
    Controller that accepts command via HTTP and
    trigger business logic layer

    Proposed POST body in JSON:
    {
      "data: "2022-08-09",
      "raw_dir": "/path/to/my_dir/raw/sales/2022-08-09"
    }
    """
    logging.basicConfig(level=logging.INFO)

    input_data: dict = request.json

    date = input_data.get('date')
    raw_dir = input_data.get('raw_dir')

    if not date:
        logging.error("date parameter missed")
        return {
            "message": "date parameter missed",
        }, 400

    logging.info(f"date: {date}, raw_dir: {raw_dir}")
    save_sales_to_local_disk(date=date, raw_dir=raw_dir)

    return {
               "message": "Data retrieved successfully from API",
           }, 201


def start_server():
    app.run(debug=True, host=SERVER_HOST, port=SERVER_PORT)


if __name__ == "__main__":
    start_server()

