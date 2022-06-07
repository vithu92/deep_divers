# ------ Importing libraries ------ #

import json
import os
# ------ Environment Variables ------ #
# Source: https://www.youtube.com/watch?v=J9QKS0NrH7I&t=277s
from base64 import b64decode

import boto3
import psycopg2  # to communicate with a PostgreSQL

# For AWS credentials

# database host
ENCRYPTED_HOST = os.environ['host']
DECRYPTED_HOST = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(ENCRYPTED_HOST),
    EncryptionContext={'LambdaFunctionName': 'tables_data_validation'}
)['Plaintext'].decode('utf-8')

# database name
ENCRYPTED_DB = os.environ['dbname']
DECRYPTED_DB = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(ENCRYPTED_DB),
    EncryptionContext={'LambdaFunctionName': 'tables_data_validation'}
)['Plaintext'].decode('utf-8')

# database user name
ENCRYPTED_USR = os.environ['user']
DECRYPTED_USR = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(ENCRYPTED_USR),
    EncryptionContext={'LambdaFunctionName': 'tables_data_validation'}
)['Plaintext'].decode('utf-8')

# user password
ENCRYPTED_PWD = os.environ['password']
DECRYPTED_PWD = boto3.client('kms').decrypt(
    CiphertextBlob=b64decode(ENCRYPTED_PWD),
    EncryptionContext={'LambdaFunctionName': 'tables_data_validation'}
)['Plaintext'].decode('utf-8')


# ------ Lambda Function ------ #

def lambda_handler(event, context):

    def connection_db():
        """
        To connect to the PostgreSQL database
        :return: Connection to the database
        """
        try:
            conn = psycopg2.connect(
                f"host={DECRYPTED_HOST} dbname={DECRYPTED_DB} user={DECRYPTED_USR} password={DECRYPTED_PWD}")
            return conn
        except psycopg2.Error as e:
            print("Error: Could not make connection to the Postgres database")
            print(e)

    # Connection to database
    conn = connection_db()

    # ----- Create validation tables and validate written data ----- #
    # Tables are called according to the table they validate: valid_TableName

    try:
        cursor = conn.cursor()

        # diet_canton_trend
        # Verify the number of cantons (ideally 26)
        cursor.execute("""DROP TABLE IF EXISTS valid_diet_canton_trend;
        CREATE TABLE valid_diet_canton_trend AS
            SELECT date_last7d, COUNT(canton) as Nb_canton
            FROM diet_canton_trend
            GROUP BY date_last7d 
            ORDER BY date_last7d DESC;""")

        # diet_search_trend
        # Verify the number of regions (ideally 3)
        cursor.execute("""DROP TABLE IF EXISTS valid_diet_search_trend;
        CREATE TABLE valid_diet_search_trend AS
            SELECT date_last7d, COUNT(DISTINCT region) as Nb_region 
            FROM diet_search_trend
            GROUP BY date_last7d 
            ORDER BY date_last7d DESC;""")

        # evolution_key_diet
        # Verify the number of entries (ideally 1 per week)
        cursor.execute("""DROP TABLE IF EXISTS valid_evolution_key_diet;
        CREATE TABLE valid_evolution_key_diet AS
            SELECT date, COUNT(*) as Nb_entries 
            FROM evolution_key_diet
            GROUP BY date 
            ORDER BY date DESC;""")

        # meat_canton_trend
        # Verify the number of cantons (ideally 26)
        cursor.execute("""DROP TABLE IF EXISTS valid_meat_canton_trend;
        CREATE TABLE valid_meat_canton_trend AS
            SELECT date_last7d, COUNT(canton) as Nb_canton
            FROM meat_canton_trend
            GROUP BY date_last7d 
            ORDER BY date_last7d DESC;""")

        # product_search
        # Verify the number of cantons (ideally 29, but not mandatory)
        cursor.execute("""DROP TABLE IF EXISTS valid_product_search;
        CREATE TABLE valid_product_search AS
            SELECT date_last7d, COUNT(DISTINCT product) as Nb_product 
            FROM product_search
            GROUP BY date_last7d 
            ORDER BY date_last7d DESC;""")

        # recipe_search
        # Verify the number of regions (ideally 3)
        cursor.execute("""DROP TABLE IF EXISTS valid_recipe_search;
        CREATE TABLE valid_recipe_search AS
            SELECT date_last7d, COUNT(DISTINCT region) as Nb_region 
            FROM recipe_search
            GROUP BY date_last7d 
            ORDER BY date_last7d DESC;""")

    except psycopg2.Error as err:
        print(f"Error: {err}")

    cursor.close()
    conn.commit()  # commit the changes
    conn.close()  # close the connection

    return {
        'statusCode': 200,
        'body': json.dumps('Validation Checks Successful')
    }
