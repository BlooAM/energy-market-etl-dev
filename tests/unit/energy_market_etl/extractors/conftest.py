import datetime as dt

import pytest


@pytest.fixture(scope='package')
def start_date():
    return dt.datetime(2023, 1, 1)


@pytest.fixture(scope='package')
def end_date():
    return dt.datetime(2023, 1, 3)
