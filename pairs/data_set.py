import os
from datetime import date
from enum import Enum

import pandas as pd
from dateutil import relativedelta
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

# Directories for data sets
dirname = os.path.dirname(__file__)
nasdaq_dir = os.path.join(dirname, '../data/nasdaq/')
nyse_dir = os.path.join(dirname, '../data/nyse/')

# .gitignore name
gitignore = '.gitignore'
# Should be ignored when reading data set
# https://seekingalpha.com/article/4082438-dryships-rank-1-worst-stock-nasdaq
known_fuck_ups = ['DRYS']


class ReadDataSetType(Enum):
    volume = 'volume'
    close = 'close'


class DataSet:
    """
    # TODO
    """
    # Create a calendar of US business days
    us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())

    # Create list of NASDAQ and NYSE years in data set
    nasdaq_years = list(map(int, [j for i, j, y in os.walk(nasdaq_dir) if j][0]))
    nyse_years = list(map(int, [j for i, j, y in os.walk(nyse_dir) if j][0]))

    # Check if data set contains same year directories
    assert len(nasdaq_years) == len(nyse_years) and sorted(nasdaq_years) == sorted(
        nyse_years), 'Exchange year data directories do not match.'

    def __init__(self, year: int = 2017, read_nasdaq: bool = True, read_nyse: bool = True,
                 illiquid_months: int = 6, illiquid_value: int = 1):
        """
        # TODO

        :param year: Year of exchange data to read.
        :param read_nasdaq: Whether nasdaq data should be read. TODO
        :param read_nyse: Whether nyse data should be read. TODO
        :param illiquid_months: Number of months to check back for averages of trading volume. Used to remove stocks
            who's average n month trading volume is < $100MM. Should use 6 or 12 months.
        :param illiquid_value: Dollar amount (in millions of dollars) used to check illiquid stocks.
        """
        self.data: pd.DataFrame = None

        self.illiquid_value = float('{:.2e}'.format(illiquid_value * 1000000))

        # Set the year
        if year and year not in list(set(self.nasdaq_years + self.nyse_years)):
            raise ValueError("Year is not in data set.")
        else:
            self.year = year
        print('Preparing data for {}'.format(self.year))

        # Which exchanges should be included in the set
        if read_nasdaq:
            # Read data files into DataFrames
            self.nasdaq: pd.DataFrame = self.read_csv_files('{}{}/'.format(nasdaq_dir, self.year))
        if read_nyse:
            self.nyse: pd.DataFrame = self.read_csv_files('{}{}/'.format(nyse_dir, self.year))

        # Set the main data set
        if not read_nasdaq and not read_nyse:
            raise Exception("Must read at least one exchange.")
        if read_nasdaq and not read_nyse:
            self.data = self.nasdaq
        if read_nyse and not read_nasdaq:
            self.data = self.nyse
        else:
            self.data = pd.concat([self.nasdaq, self.nyse], axis=0)

        # Get closing data
        _closing_data: pd.DataFrame = self.data.pivot(index='date', columns='ticker', values='close').dropna(axis=1)

        # Set volume data. Trading volume * closing price
        _volume_data = self.data.pivot(index='date', columns='ticker', values='volume')
        _volume_data = _closing_data * _volume_data.loc[:, self.closing_data.columns]
        self.volume_data: pd.DataFrame = _volume_data[_volume_data.max(axis=1) != 0]

        # Set closing data
        self.closing_data = _closing_data[_volume_data.max(axis=1) != 0]

        # Get liquid stocks that pass volume test
        self.liquid_stocks: pd.DataFrame = self.volume_data.loc[:, self.volume_data.mean(axis=0) > self.illiquid_value]

        # Compute correlations for standard liquid and scaled liquid list
        self.liquid_corr: pd.DataFrame = self.liquid_stocks.corr()

    @staticmethod
    def read_csv_files(dir_path) -> pd.DataFrame:
        """
        Reads all CSV files from dir_path into pandas DataFrame.

        :param dir_path: Full path to a year directory for an exhcange.
        :return: DataFrame of all CSVs. Transposed, with date row set to pandas datetime objects.
        """
        # Columns of the CSV data files
        data_cols = ["ticker", "date", "open", "high", "low", "close", "volume"]

        # Read in all CSV files in this directory and return DataFrame
        df = pd.concat(
            (pd.read_csv(dir_path + f, header=None, names=data_cols) for f in os.listdir(dir_path) if
             f != gitignore))

        # Change dates to pandas datetime objects
        df['date'] = pd.to_datetime(df['date'], format='%d-%b-%Y')

        return df


def get_relative_date(months: int) -> date:
    return date.today() - relativedelta.relativedelta(month=+months)


def is_business_day(check_date: date) -> bool:
    return bool(len(pd.bdate_range(check_date, check_date)))


if __name__ == '__main__':
    x = DataSet()
