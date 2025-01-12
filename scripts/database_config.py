import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

from utils.logger_config import logger


# Carregar variáveis de ambiente
load_dotenv()
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

def create_database_if_not_exists():
    try:
        # Conecta ao banco de sistema `postgres`
        conn = psycopg2.connect(
            dbname="postgres",
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Verifica se o banco de dados já existe
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (DB_NAME,))
        exists = cursor.fetchone()

        if not exists:
            print(f"Database '{DB_NAME}' does not exist. Creating...")
            # Cria o banco de dados
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
            logger.info(f"Database '{DB_NAME}' created successfully.")
        else:
            logger.info(f"Database '{DB_NAME}' already exists.")
    except Exception as e:
        logger.error(f"Error while checking/creating the database: {e}")
    finally:
        if conn:
            conn.close()

# Cria o schema e a tabela
def create_schema_and_table():
    try:
        with engine.connect() as connection:
            logger.info("Connected to the database successfully!")
            
            # Criar o schema "bronze" se não existir
            connection.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
            logger.info("Schema 'bronze' created successfully or already exists.")
    except Exception as e:
        logger.error(f"Error creating schema and table: {e}")
        raise

# Executa a verificação e criação do banco
create_database_if_not_exists()

# Cria a engine SQLAlchemy para o banco de dados
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

# Chamando a função para criar schema e tabela
create_schema_and_table()
