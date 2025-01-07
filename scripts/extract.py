import os
from dotenv import load_dotenv
import boto3
import kagglehub
from typing import List

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
        print("Connected to S3")
        return s3
    except Exception as e:
        print(e)

def get_files_from_kaggle():
    path = kagglehub.dataset_download("olistbr/brazilian-ecommerce", )

    print("Path to dataset files:", path)
    return path

def list_files(path: str) -> List[str]:
    """Lista todos os arquivos em uma pasta local."""
    files: List[str] = []
    try:
        for file_name in os.listdir(path):
            full_path = os.path.join(path, file_name)
            if os.path.isfile(full_path):
                files.append(full_path)
        print(f"Arquivos listados na pasta '{path}': {files}")
    except Exception as e:
        print(f"Erro ao listar arquivos na pasta '{path}': {e}")
        raise
    return files

def upload_files_to_bucket(files,s3):
    for file in files:
        try:
            s3.upload_file(file, BUCKET_NAME, file)
            print(f"Uploaded {file}")
        except Exception as e:
            print(f"Error uploading {file}: {e}")

def delete_local_files(local_path):
    pass

def get_files_from_bucket(s3):
    response = s3.list_objects(Bucket=BUCKET_NAME)
    if 'Contents' in response:
        for item in response['Contents']:
            print(item['Key'])
            s3.download_file(BUCKET_NAME, item['Key'], item['Key'])
            print(f"Downloaded {item['Key']}")
    else:
        print("No objects found in the bucket.")


if __name__ == "__main__":

    # Download dataset from Kaggle
    path = get_files_from_kaggle()

    # List files in the dataset folder
    files = list_files(path)

    # Connect to S3
    s3 = connect_to_s3()

    # Upload files to S3
    upload_files_to_bucket(files, s3)
