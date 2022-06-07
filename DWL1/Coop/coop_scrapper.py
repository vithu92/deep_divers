import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from pandas import DataFrame
import traceback
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import logging
import time
import json
import numpy as np
from io import StringIO
import os


def load_url_list(path) -> list:
    """
    Fetching url list from a json file
    :param path: path where the list of url is
    :return: list of strings
    """
    with open(path, "r") as json_file:
        url_list = json.load(json_file)['url']

    return url_list


def web_scrapper(url: str = None) -> BeautifulSoup:
    # Chrome Settings
    chrome_option = Options()
    chrome_option.add_argument("--headless")  # background task; don't open a window
    chrome_option.headless = True
    chrome_option.add_argument('--disable-gpu')
    chrome_option.add_argument('--no-sandbox')  # I copied this, so IDK?
    chrome_option.add_argument('--disable-dev-shm-usage')  # this too
    chrome_option.add_argument('--disable-extensions')
    # Initatlize Chrome Service and the webdriver
    chrome_service = Service(ChromeDriverManager(print_first_line=False, log_level=logging.NOTSET).install())
    chrome_service.SuppressInitialDiagnosticInformation = True
    driver = webdriver.Chrome(service=chrome_service, options=chrome_option)
    # Scrap html body
    driver.get(url)
    html = driver.page_source
    # Convert to a beautiful soup object
    return BeautifulSoup(html, "lxml")


def extract_data(url: str = None) -> pd.DataFrame:
    soup: BeautifulSoup = web_scrapper(url=url)
    json_body = json.loads(soup.find_all("meta", attrs={"data-pagecontent-json": True})[0]["data-pagecontent-json"])
    for index in range(len(json_body['anchors'])):
        if json_body['anchors'][index]['name'] == "productTile-new":
            product_data = json_body['anchors'][index]['json']['elements']

    if not product_data:
        raise ("No product data found")
    df = pd.read_json(StringIO(json.dumps(product_data)))
    if 'saving' not in df.columns:
        df['saving'] = np.nan
    if 'savingText' not in df.columns:
        df['savingText'] = np.nan

    selected_columns = ["title", "href", "quantity", "ratingValue", "price", "saving", "savingText"]

    df = df[selected_columns]

    # renaming
    if 'saving' in df.columns:
        df.rename(columns={'saving': 'oldPrice'}, inplace=True)
    if 'ratingValue' in df.columns:
        df.rename(columns={'ratingValue': 'rating'}, inplace=True)

    df['category_1'] = df['href'].apply(lambda row: row.split("/")[3])
    df['category_2'] = df['href'].apply(lambda row: row.split("/")[4])
    df['category_3'] = df['href'].apply(lambda row: row.split("/")[5])

    return df


def export_csv(df: pd.DataFrame) -> None:
    today = datetime.now()
    month = today.strftime("%m")
    day = today.strftime("%d")

    export_directory = f"./exports/{today.year}_{month}_{day}"
    isExists = os.path.exists(export_directory)
    if not isExists:
        os.makedirs(export_directory)
    filename = f"{export_directory}/{today.year}_{month}_{day}_{df.iloc[0]['category_2']}.csv"
    print(filename)
    df.to_csv(path_or_buf=filename, index=False)


def main():
    url_list: list = load_url_list("./coop_url.json")
    df_master: pd.DataFrame = None
    counter = 1
    for url in url_list:
        print("---" * 10 + f"{counter} of {len(url_list)}" + "---" * 10)
        counter += 1
        try:
            df = extract_data(url=url)
            export_csv(df=df)
        except Exception as e:
            print("Error while scrapping")
            print(f"Error message: {e}")
            break
        try:
            if not isinstance(df_master, pd.DataFrame):
                df_master = df
            else:
                df_master.append(df, ignore_index=True)
        except Exception as e:
            print(f"could not merge dataframe for the url {url}")
            print(e)
            break

    df_master.to_csv("master_csv.csv")


if __name__ == "__main__":
    main()
