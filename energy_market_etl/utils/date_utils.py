import datetime as dt
from typing import Tuple


def extract_date_components(date: dt.datetime) -> Tuple[int]:
    year = date.year
    month = date.month
    day = date.day
    return year, month, day
