import psycopg2
import pandas as pd
import re
import random
from sqlalchemy import create_engine, inspect

db_name="deep_diver_db"
db_endpoint="deep-diver-db.cvulnxnfevj3.us-east-1.rds.amazonaws.com"
user="deep_diver"
password="Amazon2022"

def get_data(tablename:str,conn=None,):
    if not conn:
        conn = psycopg2.connect(
            host=db_endpoint,
            database=db_name,
            user=user,
            password=password)  
    return pd.read_sql(f"SELECT * FROM {tablename}", conn)


def create_top_10_recipes():
    df_recipe_trend = get_data(tablename="recipe_search_trend")
    df_recipe_trend['date_last7d'] = pd.to_datetime(df_recipe_trend['date_last7d'])
    df_recipe_trend = df_recipe_trend.sort_values(by="date_last7d", ascending=False)
    df_last_date = df_recipe_trend[(df_recipe_trend['date_last7d'] == max(df_recipe_trend['date_last7d']))]
    recipe_search_list = df_last_date.sort_values(by="value", ascending=False).head(10)['query_clean'].tolist()
    
    df_recipe = get_data(tablename="recipes")
    # creating dictionary for each recipe trend word
    recipe_book = {}
    for index, row in df_recipe.iterrows():
        for product in recipe_search_list:
            if not product in recipe_book.keys():
                recipe_book[product] = []
            if product in row['label'] and row['price'] != '0':
                recipe_book[product].append(index)

    #randomly selecting recipes for each trend word
    row_selection = []
    for entry in recipe_book.keys():
        if len(recipe_book[entry]) != 0:
               row_selection.append(random.choices(recipe_book[entry])[0])

    while len(row_selection) != 10:
        entry = random.choices(list(recipe_book.keys()))[0]
        if len(recipe_book[entry]) != 0:
            row_selection.append(random.choices(recipe_book[entry])[0])
            row_selection = list(dict.fromkeys(row_selection))
    df = df_recipe.iloc[row_selection]
    return df.reset_index()

def create_top_10_product():
    df_product_trend = get_data(tablename="product_search_trend")
    df_product_trend['date_last7d'] = pd.to_datetime(df_product_trend['date_last7d'])
    df_product_trend = df_product_trend.sort_values(by="date_last7d", ascending=False)
    df_last_date = df_product_trend[(df_product_trend['date_last7d'] == max(df_product_trend['date_last7d']))]
    product_search_list = df_last_date.sort_values(by="value", ascending=False).head(10)['query_clean'].tolist()

    df_recipe = get_data(tablename="recipes")
    # creating dictionary for each recipe trend word
    product_rows = {}
    for index, row in df_recipe.iterrows():
        for entry in row['ing_quant']:
            for product in product_search_list:
                if not product in product_rows.keys():
                    product_rows[product] = []
                if product in entry:
                    product_rows[product].append(index) 

    #randomly selecting recipes for each trend word
    row_selection = []

    while len(row_selection) != 10:
        entry = random.choices(list(product_rows.keys()))[0]
        if len(product_rows[entry]) != 0:
            row_selection.append(random.choices(product_rows[entry])[0])
            row_selection = list(dict.fromkeys(row_selection))
    df = df_recipe.iloc[row_selection]
    return df.reset_index()

def formating_dataframe_by_colum(df:pd.DataFrame, column:str):
    for index, row in df.iterrows():
        entry = row[column]
        if "{" in row[column]:
            entry = row[column][1:-1]
        entry = entry.strip('\"')
        df.at[index,column] = entry
    
    return df

def get_trend_recipes():
    # diet trend
    df_diet_trend = get_data(tablename="diet_search_trend")
    df_diet_trend['date_last7d'] = pd.to_datetime(df_diet_trend['date_last7d'])
    df_diet_trend = df_diet_trend.sort_values(by="date_last7d", ascending=False)
    df_diet_trend_last_date = df_diet_trend[(df_diet_trend['date_last7d'] == max(df_diet_trend['date_last7d']))]
    diet_trend_list = df_diet_trend_last_date.sort_values(by="value", ascending=False)['diet_category'].tolist()
    if 'keto' in diet_trend_list:
        diet_trend_list.append("keto-friendly")
    
    # recipe trend
    df_recipe_trend = get_data(tablename="recipe_search_trend")
    df_recipe_trend['date_last7d'] = pd.to_datetime(df_recipe_trend['date_last7d'])
    df_recipe_trend = df_recipe_trend.sort_values(by="date_last7d", ascending=False)
    df_last_date = df_recipe_trend[(df_recipe_trend['date_last7d'] == max(df_recipe_trend['date_last7d']))]
    recipe_search_list = df_last_date.sort_values(by="value", ascending=False)['query_clean'].tolist()
    
    # product trend
    df_product_trend = get_data(tablename="product_search_trend")
    df_product_trend['date_last7d'] = pd.to_datetime(df_product_trend['date_last7d'])
    df_product_trend = df_product_trend.sort_values(by="date_last7d", ascending=False)
    df_last_date = df_product_trend[(df_product_trend['date_last7d'] == max(df_product_trend['date_last7d']))]
    product_search_list = df_last_date.sort_values(by="value", ascending=False)['query_clean'].tolist()
    
    df_recipe = get_data(tablename="recipes")
    recipe_list= []
    for index, row in df_recipe.iterrows():
        if row['price'] != "0":
            # diet trend
            if "{" in row['health_labels']:
                diet_value = row['health_labels'][1:-1].lower()
                diet_value_list = diet_value.split(",")
                for diet in diet_value_list:
                    if diet in diet_trend_list:
                        recipe_list.append(index)

            # recipe trend
            for recipe in recipe_search_list:
                if recipe in row['label']:
                    recipe_list.append(index)

            # product trend
            for entry in row['ing_quant']:
                for product in product_search_list:
                    if product in entry:
                        recipe_list.append(index)

    final_list = list(dict.fromkeys(recipe_list))
    
    df = df_recipe.iloc[final_list]
    return df.reset_index()


def main():
    # 10 top product
    df_top_products = create_top_10_product()
    engine = create_engine(
            f'postgresql://{user}:{password}@{db_endpoint}:5432/{db_name}')
    with engine.connect() as conn:
        df_top_products.to_sql(name="top_product_trend", con=conn, if_exists='replace', index=True)

    
    df = create_top_10_recipes()
    engine = create_engine(
            f'postgresql://{user}:{password}@{db_endpoint}:5432/{db_name}')
    with engine.connect() as conn:
        df.to_sql(name="top_recipe_trend", con=conn, if_exists='replace', index=True)

    # Cleaning recipe datatable
    df = get_trend_recipes()
    df = df.sample(frac = 1)
    engine = create_engine(
            f'postgresql://{user}:{password}@{db_endpoint}:5432/{db_name}')
    with engine.connect() as conn:
        df.to_sql(name="recipes_trend", con=conn, if_exists='replace', index=True)

if __name__ == '__main__':
    main()
