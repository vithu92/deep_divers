# ------ Importing libraries ------ #
import json
import psycopg2
import psycopg2.extras as extras  # to fill database
import os
import boto3
import pandas as pd
import re
import sys
import io
import numpy as np
from pandas import DataFrame # to specify type in function remove_stopw

# For stopwords
# source: https://stackoverflow.com/questions/29523254/python-remove-stop-words-from-pandas-dataframe
# (accessed: 01.05.2022)
import nltk
nltk.data.path.append("/tmp")
# https://stackoverflow.com/questions/42382662/using-nltk-corpora-with-aws-lambda-functions-in-python
# (accessed 14.05.2022)
nltk.download("punkt", download_dir="/tmp")
nltk.download("stopwords", download_dir="/tmp")
from nltk.corpus import stopwords

# for translation to English
from deep_translator import GoogleTranslator

# For AWS credentials
from boto3 import Session

# ------ Environment Variables ------ #
# Source: https://www.youtube.com/watch?v=J9QKS0NrH7I&t=277s
from base64 import b64decode

# s3_bucket_name
ENCRYPTED_BUCKET = os.environ['s3_bucket_name_dl']
DECRYPTED_BUCKET = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(ENCRYPTED_BUCKET),
    EncryptionContext={'LambdaFunctionName': 'dw_clean_insert_data'} # name of the Lambda function
)['Plaintext'].decode('utf-8')

# database host
ENCRYPTED_HOST = os.environ['host']
DECRYPTED_HOST = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(ENCRYPTED_HOST),
    EncryptionContext={'LambdaFunctionName': 'dw_clean_insert_data'}
)['Plaintext'].decode('utf-8')

# data warehouse name
ENCRYPTED_DB = os.environ['dbname_dw']
DECRYPTED_DB = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(ENCRYPTED_DB),
    EncryptionContext={'LambdaFunctionName': 'dw_clean_insert_data'}
)['Plaintext'].decode('utf-8')

# database user
ENCRYPTED_USR = os.environ['user']
DECRYPTED_USR = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(ENCRYPTED_USR),
    EncryptionContext={'LambdaFunctionName': 'dw_clean_insert_data'}
)['Plaintext'].decode('utf-8')

# database user password
ENCRYPTED_PWD = os.environ['password']
DECRYPTED_PWD = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(ENCRYPTED_PWD),
    EncryptionContext={'LambdaFunctionName': 'dw_clean_insert_data'}
)['Plaintext'].decode('utf-8')


def lambda_handler(event, context):

    # Access AWS Credentials
    # Source: https://stackoverflow.com/questions/41270571/is-there-a-way-to-get-access-key-and-secret-key-from-boto3
    session = Session()
    credentials = session.get_credentials()
    current_credentials = credentials.get_frozen_credentials()  # avoid problems related to refreshable credentials

    # Connection to S3 Bucket
    s3 = boto3.resource('s3',
                        aws_access_key_id=current_credentials.access_key,
                        aws_secret_access_key=current_credentials.secret_key,
                        aws_session_token=current_credentials.token)

    my_bucket = s3.Bucket(DECRYPTED_BUCKET)

    # Connect to the database
    def connection_db():
        """
        To create a connection to the PostgreSQL database
        :return: Connection to the database
        """

        try:
            conn = psycopg2.connect(
                f"host={DECRYPTED_HOST} dbname={DECRYPTED_DB} user={DECRYPTED_USR} password={DECRYPTED_PWD}")
            return conn
        except psycopg2.Error as err:
            print("Error: Could not make connection to the Postgres database")
            print(err)

    # Function to write data from dataframe into data table
    def insert_data(df, table):
        """
        To write data from dataframe into data table
        :param df: Pandas Dataframe
        :param table: Data table name where the data should be written
        :return: Either the print statement that the data was correctly written or the error that occured
        """
        # SOURCE: https://www.geeksforgeeks.org/how-to-write-pandas-dataframe-to-postgresql-table/

        # Connect to the data warehouse
        connection = connection_db()
        # Make a list of tuples: each value of each column in each row of the dataframe
        list_rows = [tuple(val) for val in df.to_numpy()]
        # Get the column names of the dataframe
        cols = ','.join(list(df.columns))
        # SQL query to execute in order to insert data
        # Advantage: It does not require to specify the column names
        query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)  # automatically fills with data

        try:
            cursor = connection.cursor()
            extras.execute_values(cursor, query, list_rows)  # execute the SQL query
            connection.commit() # commit the changes
            print("the dataframe is inserted")

        except (Exception, psycopg2.DatabaseError) as error:  # catch all exceptions or the database exception
            print(f"Error: {error}")
            connection.rollback()
            connection.close()

        cursor.close()
        connection.close() # close the connection


    # Translate queries to English
    def translate_en(df: DataFrame):
        """
        To translate all queries to English. 
        It detects automatically the language (source='auto') and translate it to English.
        :param df: Pandas Dataframe
        :return: The dataframe with an additional column (query_en) which contains the translated query.
        """
        # create a new column for the translated query
        df['query_en'] = ''

        for index, row in df.iterrows():
            to_translate = row['query']
            translated = GoogleTranslator(source='auto', target='en').translate(to_translate)
            # append the translation into the column
            df['query_en'].iloc[index] = translated
        return df


    # Remove stopwords
    def remove_stopw(df: DataFrame):
        """
        To remove all stop words from the translated queries. 
        The list of stop words is adjusted to the use case. It also omits adjectives and some proper names.
        :param df: Pandas Dataframe
        :return: The dataframe with an additional column (query_en_no_stopw) 
        which contains the translated query without stop words.
        """

        # specify language for stopwords
        all_stopwords = stopwords.words('english')

        # adjust the list of stopwords according to our topic
        all_stopwords += ['diet', 'diets', 'diät', 'dieta',
                          'plan', 'plans',
                          'recipes', 'recipe','recettes', 'recette', 'rezepte', 'rezept','ricette', 'ricetta',
                          'healthy', 'easy', 'vegan', 'quick', 'best', 'petite', 'petit', 'round', 'blue','homemade',
                          'special', "together", "weisses",
                          'without', 'mit',
                          'betty', 'bossi', 'betty bossi', 'thermomix', 'fooby',
                          'make', "restaurant", 'menu', "life", "sous vide", "original",
                          'oven', 'guide',  'four', 'ofen', 'forno', 'poele',
                          'im', 'in', 'au', "good", "easter", "ideas", "idea",
                          'prepare', 'dishes', 'dish', 'freeze', "cook", "cottura",
                          'picking', 'pick', 'near', "veggies", "veggies",
                          "fresh", "savory",
                          "jamie", "oliver"
                         ]
        # add stop from other languages
        all_stopwords += stopwords.words('french')
        all_stopwords += stopwords.words('italian')
        all_stopwords += stopwords.words('german')

        # clean queries to not contain any stop words
        df['query_en_no_stopw'] = df['query_en'].apply(lambda x: ' '.join([word for word in x.split() if word not in (all_stopwords)]))

        # remove empty spaces and change to lower case
        df['query_en_no_stopw'] = df['query_en_no_stopw'].str.strip().str.lower()

        # replace hyphen by space (chocolate-mousse)
        df['query_en_no_stopw'] = df['query_en_no_stopw'].str.replace('-', ' ')

        # replace empty values by NaN
        df['query_en_no_stopw'].replace('', np.nan, inplace=True)

        # drop the rows where there are no values
        df.dropna(subset=['query_en_no_stopw'], inplace = True)
        df.reset_index(inplace = True, drop = True)

        return df

    # ----- Create data tables in the data warehouse (PostgreSQL) ----- #

    conn = connection_db()
    try:
        cursor = conn.cursor()

        # diet_canton_trend
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS diet_canton_trend (canton VARCHAR(50), low_carb INT, keto INT, fasting INT, detox INT, vegan INT, date_last7d DATE, plant_based INT, planetary INT, clean INT, low_fat INT, mediterranean INT);")
        print("diet_canton_trend done")

        # evolution_key_diet
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS evolution_key_diet (date DATE, low_carb INT, keto INT, fasting INT, detox INT, vegan INT, plant_based INT, planetary INT, clean INT, low_fat INT, mediterranean INT);")
        print("evolution_key_diet done")

        # meat_canton_trend
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS meat_canton_trend (canton VARCHAR(50), beef INT, chicken INT, pork INT, seitan INT, tofu INT, date_last7d DATE);")
        print("meat_canton_trend done")

        # diet_search_trend
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS diet_search_trend (diet_category VARCHAR(100), region VARCHAR(2), date_last7d DATE, value INT);")
        print("diet_search_trend done")

        # product_search_trend
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS product_search_trend (query_clean VARCHAR(100), product VARCHAR(20), date_last7d DATE, value INT);")
        print("product_search_trend done")

        # recipe_search_trend
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS recipe_search_trend (query_clean VARCHAR(100), region VARCHAR(2), date_last7d DATE, value INT);")
        print("recipe_search_trend done")

        conn.commit()
        conn.close()

    except Exception as error:  # catch any exceptions
        print(f"Error: {error}")
        conn.rollback()
        conn.close()

    cursor.close()


    # ----- Clean data ----- #

    # Different cleaning depending on the table

    # diet_search_trend data
    def clean_diet_search_trend(df_diet: DataFrame):
        """
        To normalise the diet names and have a unique diet name for each diet name.
        :param df_diet: diet_search_trend dataframe
        :return: The dataframe with an additional column (diet_category)
        which contains the normalised and unique diet names (normalised from the column query_en_no_stopw).
        """
        # create the new column
        df_diet['diet_category'] = ""

        for index, row in df_diet.iterrows():
            # replace empty strings by NaN
            if re.match(r"^\s?$",row['query_en_no_stopw']):
                df_diet['diet_category'].iloc[index] = np.nan

            # normalise ketogene diets
            elif re.match(r".*[kc](eto)(ge)?",row['query_en_no_stopw']):
                df_diet['diet_category'].iloc[index] = "keto"

            # normalise low carb diets
            elif re.match(r".*(carb)",row['query_en_no_stopw']):
                df_diet['diet_category'].iloc[index] = "low_carb"

            # normalise planetary diet
            elif re.match(r".*(planet)",row['query_en_no_stopw']):
                df_diet['diet_category'].iloc[index] = "planetary"

            # normalise military diet
            elif re.match(r".*(milit)",row['query_en_no_stopw']):
                df_diet['diet_category'].iloc[index] = "military"

            # normalise paleo diet
            elif re.match(r".*(paleo)",row['query_en_no_stopw']):
                df_diet['diet_category'].iloc[index] = "paleo"

            # normalise egg diet
            elif re.match(r".*(egg)",row['query_en_no_stopw']):
                df_diet['diet_category'].iloc[index] = "egg"

            # normalise mediterranean diet
            elif re.match(r".*(mediter)",row['query_en_no_stopw']):
                df_diet['diet_category'].iloc[index] = "mediterranean"

            # normalise fasting diet
            elif re.match(r".*fast",row['query_en_no_stopw']):
                df_diet['diet_category'].iloc[index] = "fasting"

            # normalise detox diet
            elif re.match(r".*(detox)", row['query_en_no_stopw']):
                df_diet['diet_category'].iloc[index] = "detox"

            # normalise vegan diet
            elif re.match(r".*(vegan)", row['query_en_no_stopw']):
                df_diet['diet_category'].iloc[index] = "vegan"

            # normalise vegetarian diet
            elif re.match(r".*(veget)", row['query_en_no_stopw']):
                df_diet['diet_category'].iloc[index] = "vegetarian"

            # normalise low fat diet
            elif re.match(r".*(fat)", row['query_en_no_stopw']):
                df_diet['diet_category'].iloc[index] = "low_fat"

            # normalise plant based diet
            elif re.match(r".*(plant)", row['query_en_no_stopw']):
                df_diet['diet_category'].iloc[index] = "plant_based"

            # normalise atkins diet: it is a low carb diet
            elif re.match(r".*(atkin)", row['query_en_no_stopw']):
                df_diet['diet_category'].iloc[index] = "low_carb"

            # any other diets are formatted into lower case without any spaces
            else:
                df_diet['diet_category'].iloc[index] = df_diet['query_en_no_stopw'].iloc[index].replace(' ', '_').lower()
                df_diet['diet_category'].replace('', np.nan, inplace=True)

        # drop the rows where there are no values in diet_category
        df_diet.dropna(subset=['diet_category'], inplace = True)
        df_diet.reset_index(inplace = True, drop = True)

        # aggregate (max) to have unique values per region and per week
        df_diet = df_diet.groupby(by = ['diet_category', 'region', 'date_last7d']).max('value').reset_index()

        return df_diet

    # product_search data
    def clean_product_search(df_product: DataFrame):
        """
        To clean the product search queries by removing queries that are not related to food
        and normalising the queries as much as possible (singular/plural, word order, etc.)
        :param df_product: product_search data frame
        :return: The dataframe with an additional column (query_clean)
        which contains the normalised and cleaned queries.
        """
        # create a copy of the column query_en_no_stopw
        df_product['query_clean'] = df_product['query_en_no_stopw']

        # clean the most frequent problems

        for index, row in df_product.iterrows():

            # replace swiss german words
            # multiple if-clauses because many cleaning steps can be required for one query
            if re.match(r".*(schoggi).*",row['query_clean']):
                df_product['query_clean'].iloc[index] = re.sub(r"(schoggi)", 'chocolate ', row['query_clean'])
            if re.match(r".*(courgette).*",row['query_clean']):
                df_product['query_clean'].iloc[index] = re.sub(r"courgette", 'zucchini ', row['query_clean'])
            if re.match(r".*(moss).*",row['query_clean']):
                df_product['query_clean'].iloc[index] = re.sub(r"(moss)", 'mousse ', row['query_clean'])
            if re.match(r".*(torte).*",row['query_clean']):
                df_product['query_clean'].iloc[index] = re.sub(r"(torte)", 'pie', row['query_clean'])
            if re.match(r".*(lachs).*",row['query_clean']):
                df_product['query_clean'].iloc[index] = re.sub(r"(lachs)", 'salmon', row['query_clean'])
            if re.match(r".*(salat).*",row['query_clean']):
                df_product['query_clean'].iloc[index] = re.sub(r"(salat)", 'salad', row['query_clean'])
            if re.match(r".*(wähe).*",row['query_clean']):
                df_product['query_clean'].iloc[index] = re.sub(r"(wähe)", 'pie', row['query_clean'])

            # correct orthograph tartar
            if re.match(r".*(tartar).*",row['query_clean']):
                df_product['query_clean'].iloc[index] = re.sub(r"tartare|tartar", ' tartare ', row['query_clean'])
            if re.match(r".*(tatar).*",row['query_clean']):
                df_product['query_clean'].iloc[index] = re.sub(r"tatar", ' tartare ', row['query_clean'])

            # remove word containing "glass" (it is more about setting up a dessert)
            if re.match(r".*(glas).*",row['query_clean']):
                df_product['query_clean'].iloc[index] = re.sub(r"glass|glas", '', row['query_clean'])

            # remove row containing "grow" -> not related to cooking, but gardening
            if re.match(r".*(grow).*",row['query_clean']):
                df_product['query_clean'].iloc[index] = np.nan

            # remove when people search for information
            ## about calories of meal / product
            if re.match(r".*kcal.*|.*calorie.*|.*kalorie.*",row['query_clean']):
                df_product['query_clean'].iloc[index] = re.sub(r'.*kcal.*|.*calorie.*|.*kalorie.*', '', row['query_clean'])
            ## remove everytime they search for the cooking temperature = search for info
            if re.match(r'.*temper.*',row['query_clean']):
                df_product['query_clean'].iloc[index] = np.nan
            ## remove everytime they search for a product sold by Migros, Coop
            if re.match(r'.*migros.*',row['query_clean']):
                df_product['query_clean'].iloc[index] = np.nan
            if re.match(r'.*coop.*',row['query_clean']):
                df_product['query_clean'].iloc[index] = np.nan
            ## remove everytime they search for quantities per person
            if re.match(r'.*person.*',row['query_clean']):
                df_product['query_clean'].iloc[index] = np.nan
            # remove when people search to buy something
            if re.match(r'.*(buy).*',row['query_clean']):
                df_product['query_clean'].iloc[index] = np.nan

            # remove when look for nutritional values of a product
            if re.match(r'.*(nutri).*',row['query_clean']):
                df_product['query_clean'].iloc[index] = np.nan

            # every time it contains "make", we replace by nan and drop later the row
            # it is more about a product preparation than a recipe preparation
            # ex: making, make, maker
            if re.match(r'.*mak.*',row['query_clean']):
                df_product['query_clean'].iloc[index] = np.nan

            # every time the query contains something related to technology, we replace by nan and drop later the row
            if re.match(r'.*software.*|.*watch.*|.*tv.*|.*accounting.*|.*studio.*|.*guitar.*|.*discord.*|.*fortnite.*|.*film.*|.*machine.*|.*maschine.*',row['query_clean']):
                df_product['query_clean'].iloc[index] = np.nan

            # remove everytime a query contains a place, or when it is about translating a product into another language
            # ex: courgette deutsch, 'beef steakhouse bern'
            if re.match(r'.*switzerland.*|.*schweiz.*|.*lausanne.*|.*bar\b.*|.*suisse.*|.*france\b.*|.*deutsch.*|.*lounge.*|.*germany.*|.*french.*|.*island.*|.*city.*|.*bern.*|.*steakhouse.*|.*naples.*|.*english.*|.*world.*|.*factory.*|.*outlet.*|.*neighbors.*|.*fitness.*',row['query_clean']):
                df_product['query_clean'].iloc[index] = np.nan#re.sub(r'.*switzerland.*|.*schweiz.*|.*lausanne.*|.*bar\b.*|.*suisse.*|.*france\b.*|.*deutsch.*|.*lounge.*', '', row['query_clean'])

            # remove when they look for the hottest food
            if re.match(r".*hottest.*",row['query_clean']):
                df_product['query_clean'].iloc[index] = np.nan

            # remove noise data (skin cream)
            if re.match(r'.*(vitalisante).*',row['query_clean']):
                    df_product['query_clean'].iloc[index] = np.nan

            # every time the query contains a number, we replace by nan and drop later the row
            # ex: 350 f in c or Chilli 5000 (= scooter)
            if re.match(r'.*\d+.*',row['query_clean']):
                df_product['query_clean'].iloc[index] = np.nan

            # remove noise data in Swiss German
            # mini chuchi dini chuchi, mini chicken dini chicken
            if re.match(r'.*(dini).*',row['query_clean']):
            #if row['query_clean'] == 'mini chuchi dini chuchi':
                df_product['query_clean'].iloc[index] = np.nan
            if row['query_clean'] == 'weather bully':
                df_product['query_clean'].iloc[index] = np.nan

            # remove everytime the cleaned query is equal only to the product category = does not add any information
            try:
                if row['query_clean'].strip() == row['product']:
                    df_product['query_clean'].iloc[index] = np.nan
                # account for the special case of zucchini and courgette
                if row['query_clean'].strip() == 'zucchini':
                    df_product['query_clean'].iloc[index] = np.nan
            except:
                pass # when we have nan


            # Further specific cleaning per product category

            ## BANANA
            if row['product'] == 'banana':
            # remove noise data
            # petite, voicemeeter banana, yt, banana beauty, blue banana
                if re.match(r'\b(yt)\b|.*petite.*|.*voicemeeter.*|.*beauty.*|.*blue.*',row['query_clean']):
                    df_product['query_clean'].iloc[index] = np.nan
            # adjust word order: put pancake after
                if re.match(r'.*(pancake banana).*',row['query_clean']):
                    df_product['query_clean'].iloc[index] = re.sub(r'(pancake banana)', "banana pancake", row['query_clean'])

            ## SOUP
            if row['product'] == 'soup':
                # remove "simply soups", it is a brand = not suitable for restaurants
                if re.match(r'.*(simply).*',row['query_clean']):
                    df_product['query_clean'].iloc[index] = np.nan
            # remove soup chill, it is a brand = not suitable for restaurants
                if re.match(r'.*(chill)\b', row['query_clean']):
                    df_product['query_clean'].iloc[index] = np.nan

            ## FLAN
            if row['product'] == 'flan':
            # adjust word order: put caramel after
                if re.match(r'.*(flan caramel).*',row['query_clean']):
                    df_product['query_clean'].iloc[index] = re.sub(r'(pancake banana)', "caramel flan", row['query_clean'])

            ## MANGO
            if row['product'] == 'mango':
               # adjust word order: put dessert after
                if re.match(r'.*(dessert mango).*',row['query_clean']):
                    df_product['query_clean'].iloc[index] = re.sub(r'(dessert mango)', "mango dessert", row['query_clean'])
                # remove type of mango (people just looked for a mango name)
                if re.match(r'.*(alphonso|golden|peel).*',row['query_clean']):
                    df_product['query_clean'].iloc[index] = np.nan

            ## CAKE
            if row['product'] == 'cake':
                # remove birthday cakes queries
                if re.match(r'.*(birthday).*',row['query_clean']):
                    df_product['query_clean'].iloc[index] = np.nan

            ## FONDANT
            if row['product'] == 'fondant':
                # adjust word order: put pie after
                if re.match(r'.*(pie fondant).*',row['query_clean']):
                    df_product['query_clean'].iloc[index] = re.sub(r'(pie fondant)', "fondant pie", row['query_clean'])

            ## POTATO
            if row['product'] == 'potato':
            # remove when people look for couch potato, it is not related to food
                if re.match(r'.*(couch potat).*',row['query_clean']):
                    df_product['query_clean'].iloc[index] = np.nan
            # remove when the cell contains only potatoe or potatoes (typo / plural)
                if re.match(r'(potatoes|potatoe).*',row['query_clean']):
                    df_product['query_clean'].iloc[index] = np.nan

            ## BROCCOLI
            if row['product'] == 'brocoli':
            # remove noisy data
                if re.match(r'.*(brigadier).*',row['query_clean']): # --> does not work
                    df_product['query_clean'].iloc[index] = np.nan

            ## APPLE
            if row['product'] == 'apple':
            # remove noisy data
                if re.match(r'apple day keeps doctor away',row['query_clean']):
                    df_product['query_clean'].iloc[index] = np.nan
            # remove when it is about the Apple company
            # I need to handle this problem here, because is also means broth: would have removed important queries
                if re.match(r'.*stock.*',row['query_clean']):
                        df_product['query_clean'].iloc[index] = np.nan

            ## STRAWBERRY
            if row['product'] == 'strawberry':
            # remove when people look for strawberry legs (type of skin)
                if re.match(r'.*(legs).*',row['query_clean']):
                    df_product['query_clean'].iloc[index] = np.nan

            ## CHILLI
            if row['product'] == 'chilli':
            # remove when people look for type of vehicle
                if re.match(r'.*(trots|scooter).*',row['query_clean']):
                    df_product['query_clean'].iloc[index] = np.nan


        # remove empty spaces
        df_product['query_clean'] = df_product['query_clean'].str.strip()
        # replace double space by one
        df_product['query_clean'] = df_product['query_clean'].str.replace('  ', ' ')
        # remove empty values
        df_product['query_clean'].replace('', np.nan, inplace=True)
        # drop the rows where there are no values in query_clean
        df_product.dropna(subset=['query_clean'], inplace = True)
        df_product.reset_index(inplace = True, drop = True)

        # aggregate (max) to have only once the same value per week per product
        df_product = df_product.groupby(by = ['query_clean', 'product', 'date_last7d']).max('value').reset_index()

        return df_product


    # recipe_search data
    def clean_recipe_search(df_recipe: DataFrame):
        """
        To clean the recipe search queries by removing queries that are not related to food
        :param df_recipe: recipe_search data frame
        :return: The dataframe with an additional column (query_clean)
        which contains the normalised and cleaned queries.
        """

        # create new column
        df_recipe['query_clean'] = df_recipe['query_en_no_stopw']

        for index, row in df_recipe.iterrows():
            # remove noise data
            # cooking with kids, easter cake, swissmilk -> not appropriate for restaurant
            if re.match(r".*(easter|mother|kid|swissmilk).*",row['query_clean']):
                df_recipe['query_clean'].iloc[index] = np.nan

        # remove empty spaces
        df_recipe['query_clean'] = df_recipe['query_clean'].str.strip()
        # replace double space by one
        df_recipe['query_clean'] = df_recipe['query_clean'].str.replace('  ', ' ')
        # remove empty values
        df_recipe['query_clean'].replace('', np.nan, inplace=True)
        # drop the rows where there are no values in query_clean
        df_recipe.dropna(subset=['query_clean'], inplace = True)
        df_recipe.reset_index(inplace = True, drop = True)

        # aggregate (max) to have only once the same clean query per region and per week
        df_recipe = df_recipe.groupby(by = ['query_clean', 'region', 'date_last7d']).max('value').reset_index()

        return df_recipe


    # ----- Write data into tables ----- #

    # Category files in S3 bucket in the data lake: loop over them and load data into the data tables
    search_categories = ["diet_search_trend", "diet_canton_trend", "evolution_key_diet",
    "meat_canton_trend", "product_search", "recipe_search"]

    # Goal: For each category, access all CSV files and get only the most recent one
    # then clean and aggregate the data to write it into its proper data table in the data warehouse
    for search_category in search_categories:
        print(f"{search_category} starting")
        csv_in_category = []  # create a list of all CSVs of this search category

        # 1) Get all CSV files of this given search category
        # Source: https://www.sqlservercentral.com/articles/reading-a-specific-file-from-an-s3-bucket-using-python
        for file_name_in_category in my_bucket.objects.filter(Prefix=search_category): # select the files in bucket
            file_name = file_name_in_category.key  # get the csv file names of this search category
            if file_name.find(".csv") != -1:  # append only CSV files format into the list
                csv_in_category.append(file_name_in_category.key)

        csv_in_category.sort()  # Sort files by date
        print(f"CSV in the category: {csv_in_category}") # help to spot errors when running the code

        last_fetch_category = csv_in_category[-1]  # select only the most recent CSV file of the search category
        print(f"last fetch: {last_fetch_category}") # for verification check

        # 2) Select only the last_fetch_category of the category and write its content into data table
        obj = s3.Object(DECRYPTED_BUCKET, last_fetch_category)
        data = obj.get()['Body'].read()

        # 3) Clean and Write data into tables
        # for: diet_canton_trend, evolution_key_diet, meat_canton_trend: no further cleaning needed
        if search_category in ['diet_canton_trend', 'evolution_key_diet', 'meat_canton_trend']:
        # directly insert data into data tables
            insert_data(pd.read_csv(io.BytesIO(data), header=0, delimiter=",", low_memory=False), f'{search_category}')

        # data cleaning & normalisation depending on the table:
        else:
            data = pd.read_csv(io.BytesIO(data), header=0, delimiter=",", low_memory=False)
            # translate to english
            data = translate_en(data)
            # remove stopwords
            data = remove_stopw(data)

            # diet_search_trend
            if search_category == 'diet_search_trend':
                data = clean_diet_search_trend(data) # runs its proper function to clean and aggregate the data
                # insert data into data tables
                insert_data(data, 'diet_search_trend') # runs the function to write data into its table in the data warehouse

            # product_search
            elif search_category == 'product_search':
                data = clean_product_search(data) # runs its proper function to clean and aggregate the data
                # insert data into data tables
                insert_data(data, 'product_search_trend') # runs the function to write data into its table in the data warehouse

            # recipe_search
            elif search_category == 'recipe_search':
                data = clean_recipe_search(data) # runs its proper function to clean and aggregate the data
                # insert data into data tables
                insert_data(data, 'recipe_search_trend') # runs the function to write data into its table in the data warehouse


    return {
        'statusCode': 200,
        'body': json.dumps('Data warehouse: New Data Cleaned and Uploaded!')
    }
