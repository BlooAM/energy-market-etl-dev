import datetime as dt
import re
from typing import Tuple


FLOAT_REGEXP = re.compile(r"^[-+]?(?:\b\d+(?:\.\d*)?|\.\d+\b)(?:[eE][-+]?\d+\b)?$").match


def extract_date_components(date: dt.datetime) -> Tuple[int]:
    year = date.year
    month = date.month
    day = date.day
    return year, month, day


