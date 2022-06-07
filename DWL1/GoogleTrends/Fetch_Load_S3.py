# ------ Importing libraries ------ #

import json
import os
import time
# For Environment Variable
from base64 import b64decode
from datetime import date
from datetime import datetime
from io import StringIO

# To connect to S3 Bucket
import boto3
import pandas as pd
# To access to the AWS credentials
from boto3 import Session
from pandas import DataFrame
from pytrends.request import TrendReq  # To connect to Google Trends API

# ------ Environment Variable ------ #
# Source: https://www.youtube.com/watch?v=J9QKS0NrH7I&t=277s
ENCRYPTED_BUCKET = os.environ['s3_bucket_name']
DECRYPTED_BUCKET = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(ENCRYPTED_BUCKET),
    EncryptionContext={'LambdaFunctionName': 'Fetch_Load_S3'})['Plaintext'].decode('utf-8')


# ------ Lambda Function ------ #

def lambda_handler(event, context):
    # Access AWS Credentials
    # Source: https://stackoverflow.com/questions/41270571/is-there-a-way-to-get-access-key-and-secret-key-from-boto3
    session = Session()
    credentials = session.get_credentials()
    current_credentials = credentials.get_frozen_credentials()  # avoid problems related to refreshable credentials

    # Time definition
    today = datetime.now()
    month = today.strftime("%m")
    day = today.strftime("%d")
    htime = today.strftime("%H%M")

    def load_df_into_s3(df: DataFrame, file_name: str) -> None:
        """
        To upload a pandas Dataframe into an S3 Bucket with a timestamp when the data was fetched
        Source: https://stackoverflow.com/questions/38154040/save-dataframe-to-csv-directly-to-s3-python
        :param df: Pandas dataframe name
        :param file_name: Desired CSV file name (timestamp will be added)
        """
        bucket = DECRYPTED_BUCKET
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.Object(bucket, f"{file_name}_{today.year}_{month}_{day}_{htime}.csv").put(  # timestamp when data was fetched
            Body=csv_buffer.getvalue())  # CSV name

    def keyword_trend_time(list_food, category) -> DataFrame:
        """
        To get historical trends for Switzerland (until 2022-03-20)
        :param list_food: Search term(s), maximum 5
        :param category: Google Trends category of the search term
        :return: Dataframe with the search term(s)'s normalised measures
        """
        pytrend.build_payload(kw_list=list_food,
                              cat=category,
                              timeframe=['today 12-m', 'today 3-m', 'today 1-m'][0],
                              geo='CH',
                              gprop='')  # Web search
        data = pytrend.interest_over_time().iloc[:-1, :-1]  # :-1 (row index): we don't record the last week, -1: (col index): to remove column "isPartial"
        data.reset_index(inplace=True)
        return data

    def keyword_trend_time_7d(list_food, category) -> DataFrame:
        """
        To get trends for Switzerland over the last 7 days (starts on 2022-03-27)
        :param list_food: Search term(s), maximum 5
        :param category: Google Trends category of the search term(s)
        :return: Dataframe with the search term(s)'s normalised measures
        """
        pytrend.build_payload(kw_list=list_food,
                              cat=category,
                              timeframe=['today 12-m', 'today 3-m', 'today 1-m'][0],
                              geo='CH',
                              gprop='')
        data = pytrend.interest_over_time().iloc[-1:, :-1]  # [-1:,] To get the last 7day, [,:-1] To remove column "isPartial"
        data.reset_index(inplace=True)
        return data

    def keyword_trend_region(list_food, category) -> DataFrame:
        """
        To get search terms trend's popularity scores per canton
        :param list_food: Search term(s), maximum 5
        :param category: Google Trends category of the search term
        :return: Dataframe with the search term(s)'s normalised measures for each canton of Switzerland
        """
        pytrend.build_payload(kw_list=list_food,
                              cat=category,
                              timeframe='now 7-d',
                              geo='CH',
                              gprop='')
        data = pytrend.interest_by_region(resolution='REGION',  # canton level
                                          inc_low_vol=True,  # return data for low volume regions
                                          inc_geo_code=False)  # exclude the ISO code of the Swiss cantons
        data['date_last7d'] = [date.today() for n in range(len(data))]
        data.reset_index(inplace=True)
        return data

    # Connection to S3 Bucket
    s3 = boto3.resource('s3',
                        aws_access_key_id=current_credentials.access_key,
                        aws_secret_access_key=current_credentials.secret_key,
                        aws_session_token=current_credentials.token)
                        
    my_bucket = s3.Bucket(DECRYPTED_BUCKET)

    # Connection to Google API
    pytrend = TrendReq(tz=60) # Time Zone: Central Europe Time

    # ------ Fetching data and saving into separate CSV files ------ #

    # 1) diet_search_trend (1st CSV file category)
    # Get the diet names searched per speaking region
    kw_list = ["diät", "diet", "dieta"]  # to capture 3 language regions of Switzerland
    pytrend.build_payload(kw_list=kw_list, cat=457, geo='CH', timeframe='now 7-d')
    # cat = 457: to fetch only the category "Special & Restricted Diets"

    # Data into dataframe
    df_diets = pd.DataFrame(columns=['query', 'value', 'region', 'date_last7d'])

    for i in ["diät", "diet", "dieta"]:
        df = pytrend.related_queries()[i]['top']  # select only "top" queries (omit "rising")
        if i == 'diät':
            df['region'] = ['G' for n in range(len(df))] # specify that it is the German region
        elif i == 'diet':
            df['region'] = ['F' for n in range(len(df))] # French
        else:
            df['region'] = ['I' for n in range(len(df))] # Italian
            
        # add date when the data was fetched
        df['date_last7d'] = [date.today() for n in range(len(df))]
        df_diets = df_diets.append(df, ignore_index=True)

    # upload CSV file into the S3 Bucket with a timestamp
    load_df_into_s3(df_diets, "diet_search_trend")
    print(f"Loaded diet_search_trend at {today}")

    # 2) diet_canton_trend
    # Get the popularity evolution of key diets PER CANTON
    # We need to split the list into two lists of 5 elements due to limitations of Google Trends API
    diets5_1 = ["low carb", "keto", "fasting", "detox", "vegan"]
    diets5_2 = ["plant based", "planetary", "clean", "low fat", "mediterranean"]

    # merge the resulting dataframes
    df_diets_allRegions = keyword_trend_region(diets5_1, 457).merge(keyword_trend_region(diets5_2, 457),
                                                                    on=['geoName', 'date_last7d'])

    # Replace spaces in names
    df_diets_allRegions.rename(columns={'geoName': 'canton', 'low carb': 'low_carb',
                                        'plant based': 'plant_based', 'low fat': 'low_fat'}, inplace=True)

    load_df_into_s3(df_diets_allRegions, "diet_canton_trend")
    print(f"Loaded diet_canton_trend at {today}")

    # 3) evolution_key_diet
    # Get the popularity evolution of key diets IN SWITZERLAND
    if f"{today.year}_{month}_{day}" == "2022_03_29":
        # fetch historical data until "2022_03_29"
        df_keydiet_overtime = keyword_trend_time(diets5_1, 457).merge(keyword_trend_time(diets5_2, 457), on='date')
    else:
        # happen only the previous week
        df_keydiet_overtime = keyword_trend_time_7d(diets5_1, 457).merge(keyword_trend_time_7d(diets5_2, 457),
                                                                         on='date')
    # Replace spaces in names
    df_keydiet_overtime.rename(columns={'low carb': 'low_carb', 'plant based': 'plant_based', 'low fat': 'low_fat'},
                               inplace=True)

    load_df_into_s3(df_keydiet_overtime, "evolution_key_diet")
    print(f"Loaded evolution_key_diet at {today}")

    # 4) product_search
    # Get the related queries of food items
    # Based on the list of categories in Coop
    # We need to split the list into lists of 5 elements due to limitations of Google Trends API
    fruit = ['apple', 'banana', 'mango', 'melon', 'strawberry']
    salads_veggie = ['avocado', 'broccoli', 'endive', 'tomato', 'potato']
    more_veg = ['aubergine', 'courgette']
    herbs_sprices = ['chilli', 'garlic']
    creme_dessert = ['quark', 'flan', 'mousse', 'pudding', 'fondant']
    meat = ['beef', 'chicken', 'pork', 'seitan', 'tofu']
    pasta_cake_soup_sauce = ['pasta', 'cake', 'soup', 'bolognese', 'pesto']

    # List of these lists
    all_food = [fruit, salads_veggie, more_veg, herbs_sprices, creme_dessert, meat, pasta_cake_soup_sauce]

    # Create dataframe
    df_all_products = pd.DataFrame(columns=['query', 'value', 'date_last7d', 'product'])

    for sublist in all_food:
        pytrend.build_payload(kw_list=sublist, cat=122, geo='CH', timeframe='now 7-d')
        # cat = 122: to fetch only the category “Cooking & Recipes” 

        print('Begin:', sublist)

        for product in sublist:
            time.sleep(10)  # to avoid being blocked by Google Trends
            try:
                print(f'Fetching queries for {product}.')
                df_prod = pytrend.related_queries()[product]['top'] # top related queries
                df_prod['product'] = [product for n in range(len(df_prod))]  # add food item name in column "product"
                df_all_products = df_all_products.append(df_prod, ignore_index=True)
            except:
                print(f'No queries for {product} this week!')
                pass

    df_all_products['date_last7d'] = [date.today() for n in range(len(df_all_products))]

    load_df_into_s3(df_all_products, "product_search")
    print(f"Loaded product_search at {today}")

    # 5) recipe_search
    # Get the recipes searched per each speaking region
    pytrend.build_payload(kw_list=['recette', 'rezept', "ricetta"], cat=122, geo='CH', timeframe='now 7-d')

    df_recipe = pd.DataFrame(columns=['query', 'value', 'region', 'date_last7d'])

    for i in ['rezept', 'recette', "ricetta"]:
        df = pytrend.related_queries()[i]['top'] # select only top queries (not rising ones)
        if i == 'rezept':
            df['region'] = ['G' for n in range(len(df))]
        elif i == 'recette':
            df['region'] = ['F' for n in range(len(df))]
        else:
            df['region'] = ['I' for n in range(len(df))]
        df['date_last7d'] = [date.today() for n in range(len(df))]
        df_recipe = df_recipe.append(df, ignore_index=True)

    load_df_into_s3(df_recipe, "recipe_search")
    print(f"Loaded recipe_search at {today}")

    # 6) meat_canton_trend
    # Get the meat preferences per canton
    meat = ['beef', 'chicken', 'pork', 'seitan', 'tofu']
    df_region = keyword_trend_region(meat, 122)
    df_region.rename(columns={'geoName': 'canton'}, inplace=True)

    load_df_into_s3(df_region, "meat_canton_trend")
    print(f"Loaded meat_canton_trend at {today}")

    return {
        'statusCode': 200,
        'body': json.dumps('Fetch and Upload Into S3 Successful')
    }
