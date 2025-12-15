import pandas as pd
from airflow.sdk import task
from sqlalchemy.sql import text

from .db.connection import db_session, alchemy_engine
from .s3 import S3Adapter


@task
def upload_test_dataset():
    """
    Скачивает файл из S3 хранилища и загружаем его содержимое в таблицу.

    path_to_download_file: путь к фалу, куда будет скачиваться тестовый датасет.
    path_to_test_dataset: путь к тестовому датасету.
    test_dataset_name: название тестового датасета.
    schema_name: имя схемы в postgres, куда запишутся данные тестового датасета.
    """
    path_to_test_dataset = "/opt/app/test_dataset.csv"
    path_to_download_file = "/opt/app/DownloadedFile.csv"

    test_dataset_name = "test_dataset.csv"
    schema_name = "task_2"
    s3_adapter = S3Adapter()

    # загружаем файл в хранилище, чтобы потом его оттуда скачать
    with open(path_to_test_dataset) as file:
        s3_adapter.upload_file(test_dataset_name, file.read())

    with open(path_to_download_file, 'wb') as file:
        s3_adapter.download_file(file_name=test_dataset_name, file=file)

    with db_session() as session:
        create_schema = text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        session.execute(create_schema)
        session.commit()

    with open(path_to_download_file, 'rb') as file:
        df = pd.read_csv(file, sep="|")
        df.to_sql('test_dataset', schema=schema_name, con=alchemy_engine, if_exists='replace', index=False)
