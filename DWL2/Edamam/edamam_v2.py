import json
import pandas as pd
import requests
import psycopg2
import boto3
from typing import Dict


def lambda_handler(event, context):
    # Access to the RDS credentials
    password = get_secret()

    # Create function to gather data from Edamam Recipe Search API
    def search_recipe(search_string) -> Dict:
        """
        This fuction will let you based on the search string search
        the Edamam Recipe API and return all matching recipes in a
        dictionary
        param: search_string: string containting the search term
        return: dictionary of recipe results
        """
        url = 'https://api.edamam.com/api/recipes/v2'
        parameter = {"type": "public",
                     "q": search_string,
                     "app_id": "4ce3f5b2",
                     "app_key": "5e11edeab02666009a26dc1c3c6a50ca",
                     "random": "true"}

        response = requests.get(url, params=parameter)
        data = response.json()
        recipes = data['hits']
        all_recipe = {}

        # For each recipe
        for entry in recipes:
            entry_items = {}
            # Check whether the search query is actually within the ingredient list
            check = [True for el in entry['recipe']['ingredientLines'] if search_string in el]
            if check:
                entry_items['Image'] = entry['recipe']['image']
                entry_items['Label'] = entry['recipe']['label']
                entry_items['Url'] = entry['recipe']['url']
                entry_items['Edamam_url'] = entry['recipe']['shareAs']
                entry_items['Diets_Labels'] = entry['recipe']['dietLabels']
                entry_items['Health_Labels'] = entry['recipe']['healthLabels']
                entry_items['Ingredients'] = entry['recipe']['ingredientLines']

                ing_with_quantities = []
                ing_list = entry['recipe']['ingredients']
                for ent in ing_list:
                    ing_with_quantities.append([ent['food'], str(round(ent['weight']))])
                entry_items['Ing_Quant'] = ing_with_quantities

                entry_items['Meal_Type'] = entry['recipe']['mealType']
                entry_items['Dish_Type'] = entry['recipe']['dishType']

                all_recipe[entry_items['Label']] = entry_items

        return all_recipe

    # --------------------------------------------------------------------------
    # Total list of search queries
    fruit = ['apple', 'banana', 'mango', 'melon', 'strawberry']
    salads_veggie = ['avocado', 'broccoli', 'endive', 'tomato', 'potato']
    more_veg = ['aubergine', 'courgette']
    herbs_sprices = ['chilli', 'garlic']
    creme_dessert = ['quark', 'flan', 'mousse', 'pudding', 'fondant']
    meat = ['beef', 'chicken', 'pork', 'seitan', 'tofu']
    pasta_cake_soup_sauce = ['pasta', 'cake', 'soup', 'bolognese', 'pesto']

    all_food = [fruit, salads_veggie, more_veg, herbs_sprices, creme_dessert, meat, pasta_cake_soup_sauce]
    # --------------------------------------------------------------------------
    # Connecting to the database
    try:
        conn = psycopg2.connect(
            f"host={password['host']} dbname={password['dbname']} user={password['username']} "
            f"password={password['password']}")
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)

    # Auto commit is activated
    conn.set_session(autocommit=True)
    # --------------------------------------------------------------------------
    # Create table if not exists
    sql = """
        CREATE TABLE IF NOT EXISTS recipes (
            Label          TEXT PRIMARY KEY,
            Search_Query   TEXT NOT NULL,
            Image          TEXT,
            Url            TEXT,
            Edamam_url     TEXT NOT NULL,
            Diets_Labels   TEXT,
            Health_Labels  TEXT,
            Ingredients    TEXT,
            Ing_Quant      TEXT ARRAY,
            Meal_Type      TEXT,
            Dish_Type      TEXT) ; """

    cur.execute(sql)
    # --------------------------------------------------------------------------
    # Make searches for all items in the search query list (all_food)
    i = 0
    for li in all_food:
        for el in li:
            i += 1
            try:
                search_query = el
                d1 = search_recipe(search_query)

                # Create a Dataframe from the results
                x1 = pd.DataFrame.from_dict(d1, orient='index')
                x1 = x1.set_index("Label")
                # Add the search keyword as a column
                x1.insert(0, 'Search Query', search_query)

                # Enter the values
                sql_in = """
                INSERT INTO recipes_2 (Label, Search_Query, Image, Url, Edamam_url, Diets_Labels, Health_Labels, 
                                       Ingredients, Ing_Quant, Meal_Type, Dish_Type, Price)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """
                for i in range(len(rec_old)):
                    try:
                        values = (rec_old.iloc[i][0], rec_old.iloc[i][1], rec_old.iloc[i][2], rec_old.iloc[i][3],
                                  rec_old.iloc[i][4], rec_old.iloc[i][5], rec_old.iloc[i][6], rec_old.iloc[i][7],
                                  rec_old.iloc[i][8], rec_old.iloc[i][9], rec_old.iloc[i][10], rec_old.iloc[i][11])
                        cur.execute(sql_in, values)

                    except psycopg2.errors.UniqueViolation:
                        # In case of duplicate recipes
                        pass

            except:
                # In case of empty response
                pass

    cur.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Code run succesfully!')
    }


def get_secret():
    """
    Define function to hide credentials using Secret Manager

    return: json object containing the credentials.
    """
    # Define credential details
    secret_name = "arn:aws:secretsmanager:us-east-1:936425071459:secret:edamam_cred-qvwmBn"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # Access to the secret
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name)
    secret = get_secret_value_response['SecretString']

    return json.loads(secret)
