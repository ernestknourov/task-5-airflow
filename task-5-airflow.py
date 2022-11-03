import pandas as pd
import pendulum
import pymongo
import json
import os
from dotenv import load_dotenv
from airflow.decorators import dag, task
from pymongo.errors import BulkWriteError

load_dotenv()
USER_NAME = os.getenv('USER_NAME')
PASSWORD = os.getenv('PASSWORD')
CLUSTER_ADDRESS = os.getenv('CLUSTER_ADDRESS')
DB_NAME = os.getenv('DB_NAME')
COLLECTION_NAME = os.getenv('COLLECTION_NAME')
PATH_TO_DATA = os.getenv('PATH_TO_DATA')

@dag(
    schedule=None,
    start_date=pendulum.datetime(2022, 10, 11, tz="UTC"),
    catchup=False,
    description='task-5-airflow',
)
def pipeline() -> None:
    """
    This is pipeline for the 5th task using TaskFlow. It gets data from file,
    cleans them and push into MongoDB.
    """

    def create_connection() -> pymongo.MongoClient:
        conn_str = f"mongodb+srv://{USER_NAME}:{PASSWORD}@{CLUSTER_ADDRESS}/?retryWrites=true&w=majority"
        client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
        return client

    @task()
    def get_and_clean_data(path: str) -> str:
        """
        Gets data from csv file, then cleans them and return clear json.
        """
        df = pd.read_csv(path)
        df.dropna(axis=0, how='all', inplace=True)
        df.fillna('-', inplace=True)
        df.sort_values('at', inplace=True)
        df.replace(r'[^\w\s.,?!:\'\(\)\-]', '', regex=True, inplace=True)
        new_path = 'data.json'
        df.to_json(new_path, orient='records')
        return new_path

    @task()
    def load_data(path: str) -> None:
        """
        Pushes dataframe into MongoDB.
        """
        with open(path, 'r') as f:
            data = json.load(f)
        try:
            client = create_connection()
            db = client[f'{DB_NAME}']
            collection = db[f'{COLLECTION_NAME}']
            collection.insert_many(data)
        except BulkWriteError as bwe:
            print(bwe.details)

    path_to_csv = PATH_TO_DATA
    path_to_clear_data = get_and_clean_data(path_to_csv)
    load_data(path_to_clear_data)


pipeline()
