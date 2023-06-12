import logging
import pandas as pd
import os
import glob

# Get the logger
logger = logging.getLogger(__name__)

try:
    df_dic = pd.read_excel('statements/transactions_dictionary.xlsx', sheet_name='Dictionary')
except Exception as e:
    logger.exception("Error reading dictionary: %s", e)


def get_last_file(path):
    try:
        files = glob.glob(path + '/*')
        if not files:
            return None
        lastfile = max(files, key=os.path.getctime)
        logger.info(f'Took statement {lastfile}')
        return lastfile
    except Exception as e:
        logger.exception("Error in get_last_file: %s", e)
        return None


def read_revolut():
    try:
        file = get_last_file('statements/revolut')
        df = pd.read_csv(file)
        logger.info(f'Read {len(df)} rows from Revolut')
        df = df.rename(columns={'Started Date': 'Date'})
        df['Source'] = 'Revolut'

        # describing transactions with dictionary
        df = df.merge(df_dic, on='Description', how='left')
        df['Category'] = df['Category'].fillna('Other')

        # remove redundant columns
        df = df.drop(columns=['Type', 'Product', 'Completed Date', 'Description', 'Fee', 'State', 'Balance'])

        # correct types
        df = df.convert_dtypes()
        df['Date'] = pd.to_datetime(df['Date'], format='mixed')

        # remove negative numbers for better vizualization
        df['Is Positive Transaction'] = df['Amount'] > 0
        df['Amount'] = abs(df['Amount'])

        return df

    except Exception as e:
        logger.exception("Error reading Revolut data: %s", e)
        return pd.DataFrame()


def read_tinkoff():
    try:
        file = get_last_file('statements/tinkoff')
        df = pd.read_csv(file, sep=';', quotechar='"', encoding='windows-1251')
        logger.info(f'Read {len(df)} rows from Tinkoff')

        df = df[df['Статус'] != 'FAILED']
        df = df.rename(columns={'Дата операции': 'Date',
                                'Сумма операции': 'Amount',
                                'Валюта платежа': 'Currency',
                                'Категория': 'Category',
                                'Описание': 'Description'})
        df['Source'] = 'Tinkoff'
        df['Amount'] = df['Amount'].str.replace(',', '.').astype(float)

        df.loc[df['Category'] == 'Переводы', 'Category'] = df['Category'] + ': ' + df['Description']
        df.loc[df['Category'] == 'НКО', 'Category'] = df['Category'] + ': ' + df['Description']

        # remove internal operations
        df = df[df['Category'] != 'Переводы: Перевод между счетами']

        # describing transactions with dictionary
        df = df.drop(columns=['Description'])
        df = df.rename(columns={'Category': 'Description'})
        df = df.merge(df_dic, on='Description', how='left')
        df['Category'] = df['Category'].fillna('Other')

        # remove redundant columns
        df = df.drop(columns=['Дата платежа', 'Description', 'Номер карты', 'Сумма операции с округлением',
                              'Статус', 'Сумма платежа', 'Кэшбэк', 'MCC', 'Бонусы (включая кэшбэк)',
                              'Валюта операции', 'Округление на инвесткопилку'])

        # correct types
        df = df.convert_dtypes()
        df['Date'] = pd.to_datetime(df['Date'], format='%d.%m.%Y %H:%M:%S')

        # remove negative numbers for better vizualization
        df['Is Positive Transaction'] = df['Amount'] > 0
        df['Amount'] = abs(df['Amount'])

        return df
    except Exception as e:
        logger.exception("Error reading Tinkoff data: %s", e)
        return pd.DataFrame()


def read_moneylover():
    try:
        file = get_last_file('statements/moneylover')
        df = pd.read_csv(file, sep='\t', encoding='UTF-16')
        logger.info(f'Read {len(df)} rows from Moneylover')

        df = df.rename(columns={'Category': 'Description'})
        df['Source'] = 'Cash'

        # describing transactions with dictionary
        df = df.merge(df_dic, on='Description', how='left')
        # df['Category'] = df['Category'].fillna(df['Description'] + ': ' + df['Note'])
        df['Category'] = df['Category'].fillna('Other')

        # remove redundant columns
        df = df.drop(columns=['Id', 'Description', 'Note', 'Wallet'])

        # correct types
        df = df.convert_dtypes()
        df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')

        # remove negative numbers for better vizualization
        df['Is Positive Transaction'] = df['Amount'] > 0
        df['Amount'] = abs(df['Amount'])

        return df
    except Exception as e:
        logger.exception("Error reading Moneylover data: %s", e)
        return pd.DataFrame()


def union_statements():
    try:
        df_revolut = read_revolut()
        df_tinkoff = read_tinkoff()
        df_moneylover = read_moneylover()
        frames = [df_revolut, df_tinkoff, df_moneylover]

        df = pd.concat(frames, ignore_index=True, axis=0)
        logger.info(f'Union of statements has {len(df)} rows in total')

        return df
    except Exception as e:
        logger.exception("Error in union_statements: %s", e)
        return pd.DataFrame()
