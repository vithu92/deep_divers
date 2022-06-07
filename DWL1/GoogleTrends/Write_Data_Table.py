# ------ Importing libraries ------ #

import io
import json
import os
# ------ Environment Variables ------ #
# Source: https://www.youtube.com/watch?v=J9QKS0NrH7I&t=277s
from base64 import b64decode

import boto3
import pandas as pd
import psycopg2  # to communicate with a PostgreSQL database
import psycopg2.extras as extras  # to fill database
# For AWS credentials
from boto3 import Session

# s3_bucket_name
ENCRYPTED_BUCKET = os.environ['s3_bucket_name']
DECRYPTED_BUCKET = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(ENCRYPTED_BUCKET),
    EncryptionContext={'LambdaFunctionName': 'Write_Data_Table'}
)['Plaintext'].decode('utf-8')

# database host
ENCRYPTED_HOST = os.environ['host']
DECRYPTED_HOST = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(ENCRYPTED_HOST),
    EncryptionContext={'LambdaFunctionName': 'Write_Data_Table'}
)['Plaintext'].decode('utf-8')

# database name
ENCRYPTED_DB = os.environ['dbname']
DECRYPTED_DB = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(ENCRYPTED_DB),
    EncryptionContext={'LambdaFunctionName': 'Write_Data_Table'}
)['Plaintext'].decode('utf-8')

# database user
ENCRYPTED_USR = os.environ['user']
DECRYPTED_USR = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(ENCRYPTED_USR),
    EncryptionContext={'LambdaFunctionName': 'Write_Data_Table'}
)['Plaintext'].decode('utf-8')

# database user password
ENCRYPTED_PWD = os.environ['password']
DECRYPTED_PWD = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(ENCRYPTED_PWD),
    EncryptionContext={'LambdaFunctionName': 'Write_Data_Table'}
)['Plaintext'].decode('utf-8')


# ------ Lambda Function ------ #

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

        # Connect to the datalake
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

    # ----- Create data tables in PostgreSQL ----- #
    
    conn = connection_db()
    try:
        cursor = conn.cursor()

        # diet_canton_trend
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS diet_canton_trend (canton VARCHAR(50), low_carb INT, keto INT, fasting INT, detox INT, vegan INT, date_last7d DATE, plant_based INT, planetary INT, clean INT, low_fat INT, mediterranean INT);")
        print("Now, diet_canton_trend exists")

        # diet_search_trend
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS diet_search_trend (query VARCHAR(100), value INT, region VARCHAR(2), date_last7d DATE);")
        print("Now, diet_search_trend exists")

        # evolution_key_diet
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS evolution_key_diet (date DATE, low_carb INT, keto INT, fasting INT, detox INT, vegan INT, plant_based INT, planetary INT, clean INT, low_fat INT, mediterranean INT);")
        print("Now, evolution_key_diet exists")

        # meat_canton_trend
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS meat_canton_trend (canton VARCHAR(50), beef INT, chicken INT, pork INT, seitan INT, tofu INT, date_last7d DATE);")
        print("Now, meat_canton_trend exists")

        # product_search
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS product_search (query VARCHAR(100), value INT, date_last7d DATE, product VARCHAR(20));")
        print("Now, product_search exists")

        # recipe_search
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS recipe_search (query VARCHAR(100), value INT, region VARCHAR(2), date_last7d DATE);")
        print("Now, recipe_search exists")

        conn.commit()
        conn.close()

    except Exception as error:  # catch any exceptions
        print(f"Error: {error}")
        conn.rollback()
        conn.close()

    cursor.close()

    # ----- Write data into tables ----- #

    # Category files: loop over them and load data into the data tables
    search_categories = ["diet_canton_trend", "diet_search_trend", "evolution_key_diet",
                         "meat_canton_trend", "product_search", "recipe_search"]

    # Goal: For each category, access all CSV files and get only the most recent one
    for search_category in search_categories:
        print(f"{search_category} starting")
        csv_in_category = []  # create a list of all csv of this search category

        # 1) Get all CSV files of this given search category
        # Source: https://www.sqlservercentral.com/articles/reading-a-specific-file-from-an-s3-bucket-using-python
        for file_name_in_category in my_bucket.objects.filter(Prefix=search_category): # select the files in bucket
            file_name = file_name_in_category.key  # get the file names of this search category
            if file_name.find(".csv") != -1:  # append only CSV files into the list
                csv_in_category.append(file_name_in_category.key)

        csv_in_category.sort()  # Sort files by date
        print(f"CSV in the category: {csv_in_category}") # help to spot errors when running the code

        last_fetch_category = csv_in_category[-1]  # select only the most recent CSV file of the search category
        print(f"last fetch: {last_fetch_category}") # verification check

        # 2) Select only the last_fetch_category of the category and write its content into data table
        obj = s3.Object(DECRYPTED_BUCKET, last_fetch_category)
        data = obj.get()['Body'].read()
        insert_data(pd.read_csv(io.BytesIO(data), header=0, delimiter=",", low_memory=False), search_category)

    return {
        'statusCode': 200,
        'body': json.dumps('Data Writing Successful')
    }
