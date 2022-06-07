import boto3
import sys
import os
import pandas as pd
import csv
import io
import json
from datetime import datetime
import logging
from boto3.resources.factory import ServiceResource
import glob


def load_config(path) -> dict:
    try:
        with open(path, "r") as json_file:
            url_list = json.load(json_file)
    except Exception as e:
        logging.error(f"Could not find or read {path}")
        logging.error(e)
    return url_list


def csv_files() -> list:
    today = datetime.now()
    month = today.strftime("%m")
    day = today.strftime("%d")
    directory_path = f"./exports/{today.year}_{month}_{day}"
    return glob.glob(f'{directory_path}/*.csv')


def create_folder_on_s3(bucket_object, bucket_name: str) -> str:
    today = datetime.now()
    month = today.strftime("%m")
    day = today.strftime("%d")
    folder_name = f"{today.year}_{month}_{day}"
    try:
        bucket_object.put_object(Bucket=bucket_name, Key=(folder_name + '/'))
        return folder_name
    except Exception as e:
        logging.error("Something went wrong while creating folder on S3")
        logging.error(e)


def create_s3_connection(creds: dict, bucket_name: str) -> ServiceResource:
    s3_client = boto3.client('s3')
    s3_bucket_name = f'{bucket_name}'
    s3 = boto3.resource('s3',
                        aws_access_key_id=creds['aws_access_key_id'],
                        aws_secret_access_key=creds['aws_secret_access_key'],
                        aws_session_token=creds['aws_session_token'])
    return s3


def main():
    credentials = load_config("./credentials.json")
    bucket_name = "coop.product"
    s3 = create_s3_connection(creds=credentials, bucket_name=bucket_name)
    my_bucket = s3.Bucket(bucket_name)

    # Create Folder on S3 Bucket
    try:
        folder_name = create_folder_on_s3(bucket_object=my_bucket, bucket_name=bucket_name)
    except Exception as e:
        print(e)
        sys.exit(1)

    # Retrieving local csv files
    csv_list = csv_files()

    for csv_file in csv_list:
        print(f"uploading {csv_file} to bucket")
        csv_name = csv_file.split("\\")[-1]
        my_bucket.upload_file(os.path.abspath(csv_file), f"{folder_name}/{csv_name}")


if __name__ == "__main__":
    main()
