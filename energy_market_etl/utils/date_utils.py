import datetime as dt
from dateutil.rrule import rrule, SU, WEEKLY
import re
from typing import Tuple


FLOAT_REGEXP = re.compile(r"^[-+]?(?:\b\d+(?:\.\d*)?|\.\d+\b)(?:[eE][-+]?\d+\b)?$").match


def extract_date_components(date: dt.datetime) -> Tuple[int, int, int]:
    year = date.year
    month = date.month
    day = date.day
    return year, month, day


def get_last_sunday(year: int, month: int):
    date = dt.datetime(year=year, month=month, day=1)
    days = rrule(freq=WEEKLY, dtstart=date, byweekday=SU, count=5)
    last_rule_sunday, second_to_last_rule_sunday = days[-1], days[-2]
    return last_rule_sunday if last_rule_sunday.month == month else second_to_last_rule_sunday


def get_march_switch(year: int):
    _MARCH_MONTH_NO = 3
    day = get_last_sunday(year=year, month=_MARCH_MONTH_NO)
    return day.replace(hour=1, minute=0, second=0, microsecond=0)


def get_october_switch(year: int):
    _OCTOBER_MONTH_NO = 10
    day = get_last_sunday(year=year, month=_OCTOBER_MONTH_NO)
    return day.replace(hour=2, minute=0, second=0, microsecond=0)
