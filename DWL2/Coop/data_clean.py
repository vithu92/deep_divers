import json
import pandas as pd
import requests
import psycopg2
import boto3
from typing import Dict


def main():
    client = boto3.client('secretsmanager')

    response = client.list_secrets()
    print(response)


if __name__ == '__main__':
    main()
