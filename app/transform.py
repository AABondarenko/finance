import logging
import pandas as pd
from forex_python.converter import CurrencyRates

# Get the logger
logger = logging.getLogger(__name__)

def add_income_source(df):
    try:
        df.loc[df['Source'] == 'Revolut', 'income source'] = 'Salary'
        df.loc[df['Source'] == 'Tinkoff', 'income source'] = 'Salary'
        df.loc[(df['Source'] == 'Cash') & (
                df['Date'] >= pd.to_datetime('04/06/2023', format='%d/%m/%Y')), 'income source'] = 'Real Estate'
        df.loc[(df['Source'] == 'Cash') & (
                df['Date'] < pd.to_datetime('04/06/2023', format='%d/%m/%Y')), 'income source'] = 'Salary'
    except Exception as e:
        logger.exception("Error in add_income_source: %s", e)
    return df


def convert_currency(df, commissions=1.05):
    try:
        # TODO: convertion libraries don't provide rub rates after the war. make own rates file.
        cr = CurrencyRates()

        # Find the unique dates
        unique_dates = df[df['Currency'] == 'RUB']['Date'].dt.date.unique()

        # Fetch conversion rates for the unique dates
        rates = {}
        for date in unique_dates:
            try:
                rate = cr.get_rate('RUB', 'EUR', date)
                rates[date] = rate
            except Exception as e:
                logger.exception("Error fetching rate for date %s: %s", date, e)

        logger.info("Fetched rates: %s", rates)

        # Add % to the exchange rates
        rates = {date: rate * commissions for date, rate in rates.items()}

        def rub_to_eur(row):
            try:
                if row['Currency'] == 'RUB':
                    date = row['Date'].date()
                    rate = rates[date]  # fetch the rate from the dictionary
                    converted_amount = round(row['Amount'] * rate, 2)  # convert the amount to euros

                    logger.info("Conversion rate on %s: %s", date, rate)
                    logger.info("Amount in RUB: %s", row['Amount'])
                    logger.info("Converted amount in EUR: %s", converted_amount)

                    return converted_amount
                else:
                    return row['Amount']  # if the currency is not RUB, return the original amount
            except Exception as e:
                logger.exception("Error in rub_to_eur for row %s: %s", row, e)
                return row['Amount']  # return original amount if conversion fails

        # Apply the function to each row of the DataFrame
        df['Amount'] = df.apply(rub_to_eur, axis=1)
        df['Currency'] = 'EUR'  # update the currency column
    except Exception as e:
        logger.exception("Error in convert_currency: %s", e)

    return df
