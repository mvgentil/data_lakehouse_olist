from extract import connect_to_s3, get_files_from_kaggle, list_files
from load import process_files, clear_files, upload_files_to_bucket
from utils.logger_config import logger


def ingestion_pipeline():
    # Ingestion pipeline
    logger.info("Starting ingestion pipeline...")

    # Get files from kaggle
    path = get_files_from_kaggle()

    # List files in the dataset folder
    files = list_files(path)

    # Connect to S3
    s3 = connect_to_s3()

    # Upload files to S3
    upload_files_to_bucket(files, s3, bucket_path="raw")

    # Process files
    process_files()

    # Clear files
    clear_files("../tmp/raw")

if __name__ == "__main__":

    ingestion_pipeline()