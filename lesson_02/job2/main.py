"""
This file contains the controller that accepts command via HTTP
and trigger business logic layer
"""
import os
from flask import Flask, request
from flask import typing as flask_typing
from lesson_02.job2.local_disk import convert_json_to_avro


SERVER_HOST = os.getenv("SERVER_HOST", "localhost")
SERVER_PORT = os.getenv("SERVER_PORT", 8082)


app = Flask(__name__)


@app.route('/', methods=['POST'])
def main() -> flask_typing.ResponseReturnValue:
    input_data: dict = request.json

    raw_dir = input_data.get('raw_dir')
    stg_dir = input_data.get('stg_dir')

    print(f"raw_dir: {raw_dir}, stg_dir: {stg_dir}")

    if not raw_dir:
        return {
            "message": "raw_dir parameter missed",
        }, 400
    if not stg_dir:
        return {
            "message": "stg_dir parameter missed",
        }, 400

    convert_json_to_avro(raw_dir, stg_dir)

    return {
               "message": "Data 2 retrieved successfully from API",
           }, 201


def start_server():
    app.run(debug=True, host="localhost", port=SERVER_PORT)


if __name__ == "__main__":
    start_server()
