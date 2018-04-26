import os
import re
from datetime import date, datetime
from warnings import warn

import pandas as pd
from dateutil import relativedelta
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

# Directories for data sets
dirname = os.path.dirname(__file__)
nyse_dir = os.path.join(dirname, '../data/nyse/')
nasdaq_dir = os.path.join(dirname, '../data/nasdaq/')


class DataSet(object):
    # Create a calendar of US business days
    us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())

    # Create list of NASDAQ and NYSE years in data set
    nasdaq_years = list(map(int, [j for i, j, y in os.walk(nasdaq_dir) if j][0]))
    nyse_years = list(map(int, [j for i, j, y in os.walk(nyse_dir) if j][0]))
    # Check if data set contains same year directories
    assert len(nasdaq_years) == len(nyse_years) and sorted(nasdaq_years) == sorted(
        nyse_years), 'Exchange year data directories do not match.'

    def __init__(self, months: int = 12, year: int = 2017):
        # Set the year
        if year and year not in list(set(self.nasdaq_years + self.nyse_years)):
            raise ValueError("Year is not in data set.")
        else:
            self.year = year
        print('Preparing data for Y{}'.format(self.year))

        # Create lists of days for each exchange
        # TODO: There should be a DataFile class for these.
        self.nasdaq_day_files = [c for a, b, c in os.walk(nasdaq_dir + '/{}'.format(self.year))][0]
        self.nasdaq_days = list(map(int, [re.split('_|\.', d)[1] for d in self.nasdaq_day_files]))
        self.nasdaq_dates = [datetime.strptime(str(d), '%Y%m%d') for d in self.nasdaq_days]
        self.nyse_day_files = [c for a, b, c in os.walk(nyse_dir + '/{}'.format(self.year))][0]
        self.nyse_days = list(map(int, [re.split('_|\.', d)[1] for d in self.nyse_day_files]))
        self.nyse_dates = [datetime.strptime(str(d), '%Y%m%d') for d in self.nyse_days]

        assert len(self.nasdaq_days) == len(self.nyse_days) and sorted(self.nasdaq_days) == sorted(
            self.nyse_days), 'Exchange day sets do not match'

        # Count the expected number of trading days based off of pandas US business holiday calendar
        self.expected_trading_days = pd.DatetimeIndex(start='{}-01-01'.format(self.year),
                                                      end='{}-12-31'.format(self.year), freq=self.us_bd).size

        # Get number of trading days
        self.num_trading_days = len(list(set(self.nasdaq_days + self.nyse_days)))
        if self.expected_trading_days != self.num_trading_days:
            warn('Number of days in data set ({}) does not match the number of expected trading days ({}).'.format(
                self.num_trading_days, self.expected_trading_days))

        # TODO: user should be able to check for n months back in data set, if day n months ago is NOT a business day,
        #       get the next business day.
        # TODO: should also be able to set a date range for data
        # assert 0 < months < 24, 'Parameter months must be in range: 0 < months < 24'
        # if not is_business_day(get_relative_date(months)):
        #     pass

        # Load data
        self.data: pd.DataFrame = self.load_dataframe()

    def load_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame()
        df.index.name = 'ticker'

        # TODO: fix df has NaN values after reading in 2nd data set
        # Read data files into DataFrame
        self.df_read_data_sets('{}{}/'.format(nasdaq_dir, self.year), df)
        # self.df_read_data_sets('{}{}/'.format(nyse_dir, self.year), df)

        df_t = df.T

        return df_t

    @staticmethod
    def df_read_data_sets(dir_path: str, main_df: pd.DataFrame):
        # Columns of the CSV data files
        data_cols = ["ticker", "date", "open", "high", "low", "close", "volume"]
        # Read each record into the main DataFrame
        for file in os.listdir(dir_path):
            df = pd.read_csv(dir_path + file, header=None, names=data_cols, index_col=0)
            main_df.loc[:, df.date.iloc[0]] = df.close


def get_relative_date(months: int) -> date:
    return date.today() - relativedelta.relativedelta(month=+months)


def is_business_day(check_date: date) -> bool:
    return bool(len(pd.bdate_range(check_date, check_date)))


if __name__ == '__main__':
    x = DataSet()
