import os
from dotenv import load_dotenv
from database import engine
from extract import connect_to_s3, list_files
from typing import List
import pandas as pd

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
                print(f"Skipping empty file: {file_name}")
                continue

            if file_name.endswith(".csv"):
                # Ensure the tmp/raw/ directory exists
                base_name = os.path.basename(file_name)
                print(f"Processing {file_name}")
                local_file_path = f"tmp/raw/{base_name}"
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

                if "customers" in file_name:
                    table_name = "customers"
                elif "orders" in file_name:
                    table_name = "orders"
                elif "order_items" in file_name:
                    table_name = "order_items"
                elif "products" in file_name:
                    table_name = "products"
                elif "geolocation" in file_name:
                    table_name = "geolocation"
                elif "sellers" in file_name:
                    table_name = "sellers"
                elif "order_payments" in file_name:
                    table_name = "order_payments"
                elif "order_reviews" in file_name:
                    table_name = "order_reviews"
                elif "product_category_name_translation" in file_name:
                    table_name = "product_category_name_translation"
                else:
                    table_name = "others"
                # Download and process the file
                s3.download_file(BUCKET_NAME, file_name, local_file_path)
                print(f"Downloaded {file_name}")

                print(f"Processing {file_name} to {table_name}")
                pd.read_csv(local_file_path).to_sql(table_name, engine, if_exists='append', index=False, schema='bronze')

                
                move_file_to_processed(file_name)

                print(f"Processed {file_name}")

        print("File processing completed.")

def move_file_to_processed(file_name: str):
    base_name = os.path.basename(file_name)
    s3.copy_object(
        Bucket=BUCKET_NAME,
        CopySource=f"{BUCKET_NAME}/{file_name}",
        Key=f"processed/{base_name}"
    )
    s3.delete_object(Bucket=BUCKET_NAME, Key=file_name)
    print(f"Moved {file_name} to processed folder")

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
    print("Removing files...")
    for file in files:
        os.remove(file)
    print("Files removed from tmp folder.")


if __name__ == "__main__":

    process_files()
    clear_files("tmp/raw")
