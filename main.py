import logging
from app import get_data, transform, upload

# Configure the logger
logging.basicConfig(filename='app.log',
                    filemode='w',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S',
                    level=logging.INFO)

logger = logging.getLogger(__name__)


def main():
    get_data.clear_files()
    df = get_data.union_statements()
    df = transform.convert_currency(df, commissions=1.07)
    df = transform.add_income_source(df)
    upload.to_postgres(df)


if __name__ == '__main__':
    main()
