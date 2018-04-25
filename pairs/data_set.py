import os
from datetime import date
import re
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

    # Get that most recent day for the most current year
    # List of days in NASDAQ set for most recent year
    # nasdaq_days_set = [c for a, b, c in os.walk(nasdaq_dir + '/{}'.format(max(nasdaq_years)))][0]
    # nasdaq_most_recent_day = max(list(map(int, [re.split('_|\.', d)[1] for d in nasdaq_days_set])))
    # List of days in NYSE set for most recent year
    # nyse_days_set = [c for a, b, c in os.walk(nyse_dir + '/{}'.format(max(nyse_years)))][0]
    # nyse_most_recent_day = max(list(map(int, [re.split('_|\.', d)[1] for d in nyse_days_set])))
    # Make sure the days are the same
    # assert nasdaq_most_recent_day == nyse_most_recent_day, 'Exchange most recent days do not match.'

    def __init__(self, nyse: bool = True, nasdaq: bool = True, months: int = 12, year: int = 2017):
        # Set the year
        if year and year not in list(set(self.nasdaq_years + self.nyse_years)):
            raise ValueError("Year is not in data set.")
        else:
            self.year = year

        # Create lists of days for each exchange
        self.nasdaq_day_files = [c for a, b, c in os.walk(nasdaq_dir + '/{}'.format(max(self.nasdaq_years)))][0]
        self.nyse_day_files = [c for a, b, c in os.walk(nyse_dir + '/{}'.format(max(self.nyse_years)))][0]
        self.nasdaq_days = list(map(int, [re.split('_|\.', d)[1] for d in self.nasdaq_day_files]))
        self.nyse_days = list(map(int, [re.split('_|\.', d)[1] for d in self.nyse_day_files]))

        assert len(self.nasdaq_days) == len(self.nyse_days) and sorted(self.nasdaq_days) == sorted(
            self.nyse_days), 'Exchange day sets do not match'

        # Count the expected number of trading days based off of pandas US business holiday calendar
        self.expected_trading_days = pd.DatetimeIndex(start='{}-01-01'.format(self.year),
                                                      end='{}-12-31'.format(self.year), freq=self.us_bd).size

        self.num_trading_days = len(list(set(self.nasdaq_days + self.nyse_days)))

        # Read nyse data
        if nyse:
            pass
        # Read nasdaq data
        if nasdaq:
            pass
        #
        assert 0 < months < 24, 'Parameter months must be in range: 0 < months < 24'

        # TODO: user should be able to check for n months back in data set, if day n months ago is NOT a business day,
        #       get the next business day.
        if not is_business_day(get_relative_date(months)):
            pass


def get_relative_date(months: int) -> date:
    return date.today() - relativedelta.relativedelta(month=+months)


def is_business_day(check_date: date) -> bool:
    return bool(len(pd.bdate_range(check_date, check_date)))


if __name__ == '__main__':
    x = DataSet()
