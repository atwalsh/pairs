import os
import re
from datetime import date, datetime
from enum import Enum
from warnings import warn

import pandas as pd
from dateutil import relativedelta
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
from sklearn.preprocessing import Imputer, StandardScaler

# Directories for data sets
dirname = os.path.dirname(__file__)
nasdaq_dir = os.path.join(dirname, '../data/nasdaq/')
nyse_dir = os.path.join(dirname, '../data/nyse/')

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

    def __init__(self,  year: int = 2017, read_nasdaq: bool = True, read_nyse: bool = True,
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
        # Which exchanges should be included in the set
        # if read_nasdaq:
        #   TODO
        # if read_nyse:
        #   TODO

        # Set illiquid stock check fields
        self.illiquid_months = illiquid_months
        self.illiquid_value = float('{:.2e}'.format(illiquid_value * 1000000))

        # Set the year
        if year and year not in list(set(self.nasdaq_years + self.nyse_years)):
            raise ValueError("Year is not in data set.")
        else:
            self.year = year
        print('Preparing data for {}'.format(self.year))

        # Create lists of days for each exchange
        # TODO: There should be a DataFile class for these.
        self.nasdaq_day_files = [x for a, b, c in os.walk(nasdaq_dir + '/{}'.format(self.year)) for x in c if
                                 x != '.gitignore']
        self.nasdaq_days = list(map(int, [re.split('_|\.', d)[1] for d in self.nasdaq_day_files]))
        self.nasdaq_dates = [datetime.strptime(str(d), '%Y%m%d') for d in self.nasdaq_days]
        self.nyse_day_files = [x for a, b, c in os.walk(nyse_dir + '/{}'.format(self.year)) for x in c if
                               x != '.gitignore']
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

        # Load closing price and volume data set
        self.closing_price_data: pd.DataFrame = self.load_dataframe(ReadDataSetType.close)
        self.volume_data: pd.DataFrame = self.load_dataframe(ReadDataSetType.volume)

        # Get liquid stocks that pass volume test
        self.liquid_stocks: pd.DataFrame = self.volume_data.loc[:, self.volume_data.mean(axis=0) > self.illiquid_value]

        # Create scaled liquid stocks
        self.scaled_liquid_stocks: pd.DataFrame = self.create_scaled_stocks_df()

        # Compute correlations for standard liquid and scaled liquid list
        self.liquid_corr: pd.DataFrame = self.liquid_stocks.corr()
        self.scaled_liquid_corr: pd.DataFrame = self.scaled_liquid_stocks.corr()

    def load_dataframe(self, data_type: ReadDataSetType) -> pd.DataFrame:
        """
        Creates a main DataFrame from all data sets then transposes the index and columns.

        :return: Transposed DataFrame of all CSV data files.
        """
        # Read data files into DataFrames
        nasdaq_df = self.df_read_data_sets('{}{}/'.format(nasdaq_dir, self.year), data_type=data_type)
        nyse_df = self.df_read_data_sets('{}{}/'.format(nyse_dir, self.year), data_type=data_type)

        # Concat DataFrames into main result DataFrames
        main_df = pd.concat([nasdaq_df, nyse_df], axis=0)

        # Transpose DataFrame
        main_df = main_df.T

        # Change column name from ticker to date
        main_df.columns.name = 'date'

        # Change index to pandas datetime objects
        main_df.index = pd.to_datetime(main_df.index)

        # Clear bad stocks
        for s in known_fuck_ups:
            main_df.drop(s, axis=1, inplace=True)

        # Return transposed DataFrame with dates as index and ticker symbols as columns
        return main_df

    def create_scaled_stocks_df(self) -> pd.DataFrame:
        imputer = Imputer().fit(X=self.liquid_stocks)
        imputed_companies = pd.DataFrame(imputer.transform(self.liquid_stocks), index=self.liquid_stocks.index,
                                         columns=self.liquid_stocks.columns)
        scaler = StandardScaler().fit(imputed_companies)
        scaled_companies = pd.DataFrame(scaler.transform(imputed_companies), index=imputed_companies.index,
                                        columns=imputed_companies.columns)

        return scaled_companies

    @staticmethod
    def df_read_data_sets(dir_path: str, data_type: ReadDataSetType) -> pd.DataFrame:
        """
        Read a directory of CSV data files into a DataFrame.

        :param data_type:
        :param dir_path: Full path of directory.
        :return: DataFrame of all CSV data files in directory.
        """
        closing_df = pd.DataFrame()
        volume_df = pd.DataFrame()
        closing_df.index.name = 'ticker'
        volume_df.index.name = 'volume'

        # Columns of the CSV data files
        data_cols = ["ticker", "date", "open", "high", "low", "close", "volume"]

        # Read each record into the main DataFrame
        for file in os.listdir(dir_path):
            # Read CSV
            df = pd.read_csv(dir_path + file, header=None, names=data_cols, index_col=0)
            closing_df.loc[:, df.date.iloc[0]] = df.close
            volume_df.loc[:, df.date.iloc[0]] = df.volume

        if data_type == ReadDataSetType.volume:
            volume_df = volume_df * closing_df

        return closing_df if data_type == ReadDataSetType.close else volume_df


def get_relative_date(months: int) -> date:
    return date.today() - relativedelta.relativedelta(month=+months)


def is_business_day(check_date: date) -> bool:
    return bool(len(pd.bdate_range(check_date, check_date)))


if __name__ == '__main__':
    x = DataSet(illiquid_value=100)
