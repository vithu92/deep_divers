import pandas as pd
import glob
import logging
from sqlalchemy import create_engine, inspect
from datetime import datetime


def upload_dataframe_to_table(table_name: str, df: pd.DataFrame, username: str = "deep_diver",
                              password: str = "Amazon2022"):
    engine = create_engine(
        f'postgresql://{username}:{password}@deepdiver.ca7k4yfxv4gc.us-east-1.rds.amazonaws.com:5432/deep_diver_db')
    with engine.connect() as conn:

        # df_before = df.shape
        table_exists = False
        # Check if table already exists
        ins = inspect(engine)
        for _t in ins.get_table_names():
            if table_name in _t:
                table_exists = True
        if table_exists:
            sql_query = f"alter table public.\"Product\" alter column \"savingText\" type text;"
            engine.execute(sql_query)
            df.to_sql(name=f'{table_name}', con=engine, if_exists='replace', index=False)
        #     # read table
        #     sql_query = f"SELECT * FROM public.\"{table_name}\""
        #     sql_df = pd.read_sql(sql=sql_query, con=conn)
        #     df = pd.concat((df, sql_df), ignore_index=True).drop_duplicates(keep=False)
        #     df.drop(['index'], axis=1, inplace=True)
        #     df_after = df.shape
        #     removed_rows = df_before[0]-df_after[0]
        #     print(f"Total {removed_rows} rows has been removed because they exists already in DB")
        else:
            df.to_sql(name=f'{table_name}', con=engine, if_exists='replace', index=False)

        logging.info(f"Connetion Status: {bool(conn)}")


def csv_files() -> list:
    today = datetime.now()
    month = today.strftime("%m")
    day = today.strftime("%d")
    directory_path = f"./Coop/exports/{today.year}_{month}_{day}"
    return glob.glob(f'{directory_path}/*.csv')


def main():
    csv_list = csv_files()
    counter = 1

    df = pd.read_csv("master_csv.csv")
    # df = df.drop(["Unnamed: 0"], axis=1)
    try:
        upload_dataframe_to_table(table_name="Product", df=df)
    except Exception as e:
        logging.error(f"Could not upload dataframe to db")
        logging.error(e)
    # for csv in csv_list:
    #     print("---" * 10 + f"uploading {counter} of {len(csv_list)} to DB" + "---" * 10)
    #     counter += 1
    #     df = pd.read_csv(csv)
    #     try:
    #         upload_dataframe_to_table(table_name="Product", df=df)
    #     except Exception as e:
    #         logging.error(f"Could not upload dataframe to db")
    #         logging.error(e)
    #         break


if __name__ == "__main__":
    main()
