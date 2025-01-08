import psycopg2
import os
from dotenv import load_dotenv
from extract import connect_to_s3, get_files_from_bucket, list_files
import duckdb
from typing import List
import pandas as pd

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
BUCKET_NAME = os.getenv('BUCKET_NAME')

POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')


# Configurações do S3
s3 = connect_to_s3()


def file_already_processed(file_name, cursor):
    query = "SELECT 1 FROM arquivos_processados WHERE nome_arquivo = %s;"
    cursor.execute(query, (file_name,))
    return cursor.fetchone() is not None

def process_files():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    cursor = conn.cursor()

    # Cria a tabela de controle se não existir
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS arquivos_processados (
            id SERIAL PRIMARY KEY,
            nome_arquivo TEXT UNIQUE,
            data_processamento TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()

    # Lista os arquivos no bucket S3
    response = s3.list_objects_v2(Bucket=BUCKET_NAME)

    for obj in response.get("Contents", []):
        file_name = obj["Key"]

        # Verifica se o arquivo já foi processado
        if not file_already_processed(file_name, cursor):
            # Baixa e processa o arquivo
            s3.download_file(BUCKET_NAME, file_name, f"tmp/raw/{file_name}")
            print(f"Downloaded {file_name}")

            
            # Marca o arquivo como processado
            cursor.execute(
                "INSERT INTO arquivos_processados (nome_arquivo) VALUES (%s);",
                (file_name,),
            )
            conn.commit()

    cursor.close()
    conn.close()

def transform_csv_to_parquet(files: List[str], s3):
    for file in files:
        try:
            base_name = os.path.basename(file).split(".")[0]
            parquet_file_path = f"tmp/parquet/{base_name}.parquet"
            print(parquet_file_path)
            print(f"Transforming {file} to {parquet_file_path}")

            try:
                pd.read_csv(file).to_parquet(f'tmp/parquet/{base_name}.parquet')
            except Exception as e:
                print(f"Error detected for {file}: {e}")
                continue

            print(f"Transformed {file} to {parquet_file_path}")
        except Exception as e:
            print(f"Error processing {file}: {e}")

def upload_files_to_bucket(files: List[str], s3, bucket_path: str):
    for file in files:
        try:
            file_name = os.path.basename(file)
            if "customers" in file_name:
                folder = "customers"
            elif "orders" in file_name:
                folder = "orders"
            elif "order_items" in file_name:
                folder = "order_items"
            elif "products" in file_name:
                folder = "products"
            elif "geolocation" in file_name:
                folder = "geolocation"
            elif "sellers" in file_name:
                folder = "sellers"
            elif "order_payments" in file_name:
                folder = "order_payments"
            elif "order_reviews" in file_name:
                folder = "order_reviews"
            elif "product_category_name_translation" in file_name:
                folder = "product_category_name_translation"
            else:
                folder = "others"

            print(f'Starting upload file: {file_name} to folder: {folder}')

            s3_key = f"{bucket_path}/{folder}/{file_name}"
            s3.upload_file(file, BUCKET_NAME, s3_key)
            print(f"Uploaded {file_name} to {BUCKET_NAME}/{s3_key}")
        except Exception as e:
            print(f"Error uploading {file_name}: {e}")

def clear_files(path: str):
    files = list_files(path)
    for file in files:
        os.remove(file)



if __name__ == "__main__":

    get_files_from_bucket(s3, "tmp", "raw/")
    
    raw_files = list_files("tmp/raw/")

    clear_files("tmp/raw")

