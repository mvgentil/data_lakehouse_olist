import os
from dotenv import load_dotenv
import boto3
import kagglehub
from typing import List
import pandas as pd

from utils.logger_config import logger

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
BUCKET_NAME = os.getenv('BUCKET_NAME')

KAGGLE_USERNAME = os.getenv('KAGGLE_USERNAME')
KAGGLE_KEY = os.getenv('KAGGLE_KEY')


def connect_to_s3():
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION)
        logger.info("Connected to S3")
        return s3
    except Exception as e:
        logger.error(e)

def get_files_from_kaggle() -> str:
    logger.info("Downloading dataset from Kaggle...")
    path = kagglehub.dataset_download("olistbr/brazilian-ecommerce", )

    logger.info("Path to dataset files:", path)
    return path

def list_files(path: str) -> List[str]:
    """Lista todos os arquivos em uma pasta local."""
    logger.info(f"Listing files in folder '{path}'")
    files: List[str] = []
    try:
        for file_name in os.listdir(path):
            full_path = os.path.join(path, file_name)
            if os.path.isfile(full_path):
                files.append(full_path)
        logger.info(f"Files in folder '{path}': {files}")
    except Exception as e:
        logger.error(f"Error listing files in folder '{path}': {e}")
        raise
    return files

def upload_files_to_bucket(files: List[str], s3, bucket_path: str):
    logger.info(f"Uploading files to bucket '{BUCKET_NAME}'")
    for file in files:
        try:
            logger.info(f"Uploading file '{file}' to bucket '{BUCKET_NAME}'")
            file_name = os.path.basename(file)
            s3_key = f"{bucket_path}/{file_name}"
            s3.upload_file(file, BUCKET_NAME, s3_key)
            logger.info(f"Uploaded {file_name} to {BUCKET_NAME}/{s3_key}")
        except Exception as e:
            logger.info(f"Error uploading {file_name}: {e}")


def get_files_from_bucket(s3, local_path: str, bucket_path: str):
    try:
        logger.info(f"Downloading objects from bucket '{BUCKET_NAME}'")
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=bucket_path)
        if 'Contents' in response:
            for obj in response['Contents']:
                file_key = obj['Key']

                if file_key == bucket_path or obj['Size'] == 0:
                    print(f"Skipping {file_key} (folder or empty object)")
                    continue
                
                logger.info(f"Downloading: {file_key}")
                local_file_path = os.path.join(local_path, file_key)
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                s3.download_file(BUCKET_NAME, file_key, local_file_path)
                logger.info(f"Downloaded {file_key} to {local_file_path}")
        else:
            logger.info("No objects found in the bucket.")
    except Exception as e:
        logger.error(f"Error downlaoding objects from bucket: {e}")

def normalize_csv(file_path: str, output_path: str):
    try:
        logger.info(f"Normalizing {file_path}")
        df = pd.read_csv(file_path, encoding='ISO-8859-1')  # Adjust encoding if necessary
        df.to_parquet(output_path, index=False, encoding='UTF-8')
        logger.info(f"Normalized {file_path} to {output_path}")
    except Exception as e:
        logger.error(f"Error normalizing {file_path}: {e}")


if __name__ == "__main__":

    # Download dataset from Kaggle
    path = get_files_from_kaggle()

    # List files in the dataset folder
    files = list_files(path)

    # Connect to S3
    s3 = connect_to_s3()

    # Upload files to S3
    upload_files_to_bucket(files, s3, bucket_path="raw")