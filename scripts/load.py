import os
from dotenv import load_dotenv
from database_config import engine
from extract import connect_to_s3, list_files
from typing import List
import pandas as pd
import sys

sys.path.append('../')
from utils.logger_config import logger

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
BUCKET_NAME = os.getenv('BUCKET_NAME')

# Configurações do S3
s3 = connect_to_s3()

def process_files():
    with engine.begin() as connection:

        # List files in the S3 bucket
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="raw/")
        
        for obj in response.get("Contents", []):
            file_name = obj["Key"]
            if obj["Size"] == 0:
                logger.info(f"Skipping empty file: {file_name}")
                continue

            if file_name.endswith(".csv"):
                # Ensure the tmp/raw/ directory exists
                base_name = os.path.basename(file_name)
                logger.info(f"Processing {file_name}")
                local_file_path = f"../tmp/raw/{base_name}"
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

                # Define mapping of file patterns to table names
                table_mapping = {
                    "customers": "customers",
                    "orders": "orders",
                    "order_items": "order_items",
                    "products": "products",
                    "geolocation": "geolocation",
                    "sellers": "sellers",
                    "order_payments": "order_payments",
                    "order_reviews": "order_reviews",
                    "product_category_name_translation": "product_category_name_translation"
                }

                # Find matching table name from file name
                table_name = next((value for key, value in table_mapping.items() 
                                 if key in file_name), None)
                
                if table_name is None:
                    logger.info(f"Skipping file {file_name} (not recognized)")
                    continue

                logger.info(f"Table name: {table_name}")

                # Download and load data to databse
                s3.download_file(BUCKET_NAME, file_name, local_file_path)
                logger.info(f"Downloaded {file_name}")

                logger.info(f"Loading {file_name} to {table_name}")
                pd.read_csv(local_file_path).to_sql(table_name, engine, if_exists='replace', index=False, schema='bronze')

                
                move_file_to_processed(file_name)

                logger.info(f"Processed {file_name}")

        logger.info("File processing completed.")

def move_file_to_processed(file_name: str):
    base_name = os.path.basename(file_name)
    logger.info(f"Moving {file_name} to processed folder")
    s3.copy_object(
        Bucket=BUCKET_NAME,
        CopySource=f"{BUCKET_NAME}/{file_name}",
        Key=f"processed/{base_name}"
    )
    s3.delete_object(Bucket=BUCKET_NAME, Key=file_name)
    logger.info(f"Moved {file_name} to processed folder")

def transform_csv_to_parquet(files: List[str], s3):
    for file in files:
        try:
            base_name = os.path.basename(file).split(".")[0]
            parquet_file_path = f"tmp/parquet/{base_name}.parquet"
            logger.info(f"Transforming {file} to {parquet_file_path}")
            try:
                pd.read_csv(file).to_parquet(f'tmp/parquet/{base_name}.parquet')
            except Exception as e:
                logger.info(f"Error detected for {file}: {e}")
                continue

            logger.info(f"Transformed {file} to {parquet_file_path}")
        except Exception as e:
            logger.info(f"Error processing {file}: {e}")

def upload_files_to_bucket(files: List[str], s3, bucket_path: str):
    for file in files:
        try:
            file_name = os.path.basename(file)
            
            # Define mapping of file patterns to folder names
            folder_mapping = {
                "customers": "customers",
                "orders": "orders",
                "order_items": "order_items",
                "products": "products",
                "geolocation": "geolocation",
                "sellers": "sellers",
                "order_payments": "order_payments",
                "order_reviews": "order_reviews",
                "product_category_name_translation": "product_category_name_translation"
            }

            # Find matching folder name from file name
            folder = next((value for key, value in folder_mapping.items() 
                         if key in file_name), "others")

            logger.info(f'Starting upload file: {file_name} to folder: {folder}')

            s3_key = f"{bucket_path}/{folder}/{file_name}"
            s3.upload_file(file, BUCKET_NAME, s3_key)
            logger.info(f"Uploaded {file_name} to {BUCKET_NAME}/{s3_key}")
        except Exception as e:
            logger.info(f"Error uploading {file_name}: {e}")

def clear_files(path: str):
    files = list_files(path)
    if len(files) == 0 or files is None:
        logger.info("No files to remove.")
    else:
        logger.info(f"Files to remove: {len(files)}")
        logger.info("Removing files...")
        for file in files:
            os.remove(file)
        logger.info("Files removed from tmp folder.")


if __name__ == "__main__":

    process_files()
    clear_files("../tmp/raw")
