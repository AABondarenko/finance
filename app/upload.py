import logging
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Get the logger
logger = logging.getLogger(__name__)


def to_postgres(df):
    try:
        load_dotenv()
        pg_user = os.getenv('PG_USER')
        pg_pass = os.getenv('PG_PASS')
        pg_serv = os.getenv('PG_SERV')
        pg_port = os.getenv('PG_PORT')
        pg_dtbs = os.getenv('PG_DTBS')
        if not all([pg_user, pg_pass, pg_serv, pg_port, pg_dtbs]):
            raise ValueError('One or more database credentials are missing')

        df.columns = df.columns.str.lower()

        engine = create_engine(
            f'postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_serv}:{pg_port}/{pg_dtbs}')
        df.to_sql('transactions', engine, if_exists='replace')
        logging.info(f'{df.shape[0]} rows uploaded to Postgres')

    except ValueError as ve:
        logging.error('ValueError:', ve)
    except Exception as e:
        logging.error('An error occurred:', e)
