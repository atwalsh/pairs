import datetime
import os
from datetime import date

import pandas as pd
from dateutil import relativedelta
from pandas import DataFrame
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

nyse_dir = '../data/nyse'
nasdaq_dir = '../data/nasdaq'


class DataSet(DataFrame):
    us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())
    nasdaq_years: list(map(int, [j for i, j, y in os.walk(nasdaq_dir) if j][0]))
    nyse_years: list(map(int, [j for i, j, y in os.walk(nyse_dir) if j][0]))

    def __init__(self, nyse: bool = True, nasdaq: bool = True, months: int = 12, year: datetime.date.year = None,
                 start_day: date = None):
        self.latest_nyse_date = None
        self.latest_nasdaq_date = None
        self.num_calendar_days = None
        self.num_trading_days = 0
        self.expected_trading_days = 0
        # Read nyse data
        if nyse:
            pass
        # Read nasdaq data
        if nasdaq:
            pass
        #
        assert 0 < months < 24, 'parameter months must be in range: 0 < months < 24'

        # TODO: user should be able to check for n months back in data set, if day n months ago is NOT a business day,
        #       get the next business day.
        if not is_business_day(get_relative_date(months)):
            pass
        super().__init__()


def get_relative_date(months: int) -> date:
    return date.today() - relativedelta.relativedelta(month=+months)


def is_business_day(check_date: date) -> bool:
    return bool(len(pd.bdate_range(check_date, check_date)))
