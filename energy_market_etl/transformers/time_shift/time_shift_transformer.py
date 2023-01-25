import datetime as dt
import logging
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd

from energy_market_etl.transformers.transformer import Transformer
from energy_market_etl.utils.date_utils import get_march_switch, get_october_switch


class TimeShiftTransformer(Transformer):
    _GROUP_COLUMNS = ['Doba', 'Data']
    _INDEX_COLUMNS = ['Godzina']

    def __init__(self, aggregate_function: Union[str, Callable] = 'mean') -> None:
        self.aggregate_function = aggregate_function

    def transform(self, raw_data_snapshots: Dict[dt.datetime, pd.DataFrame]) -> Dict[dt.datetime, pd.DataFrame]:
        data_snapshots = {}
        for datetime, raw_data_snapshot in raw_data_snapshots.items():
            current_date = datetime.date()
            current_year = datetime.year

            if current_date == get_march_switch(year=current_year).date():
                logging.info(
                    f'March time shift detected on date: {current_date}. Missing information being interpolated...'
                )
                data_snapshots[datetime] = self.__handle_time_shift(
                    raw_data_snapshot=raw_data_snapshot,
                    input_columns=['1', '3'],
                    output_column='2',
                )
            elif current_date == get_october_switch(year=current_year).date():
                logging.info(
                    f'October time shift detected on date: {current_date}. Additional information being aggregated...'
                )
                data_snapshots[datetime] = self.__handle_time_shift(
                    raw_data_snapshot=raw_data_snapshot,
                    input_columns=['2', '2A'],
                    output_column='2',
                    columns_to_drop='2A',
                )
            else:
                data_snapshots[datetime] = raw_data_snapshots[datetime].copy()

        return data_snapshots

    def __handle_time_shift(
            self,
            raw_data_snapshot: pd.DataFrame,
            input_columns: List[str],
            output_column: str,
            columns_to_drop: Union[str, List[str]] = '',
    ) -> pd.DataFrame:
        data_snapshot = raw_data_snapshot.copy()
        index_column, group_column = TimeShiftTransformer.__get_index_group_column(data_snapshot.columns)
        group_column_value = data_snapshot[group_column].unique()[0]
        apply_transposition = False if set(input_columns) <= set(data_snapshot.columns) else True

        if apply_transposition:
            data_snapshot[index_column] = data_snapshot[index_column].apply(str)
            data_snapshot = data_snapshot.drop(group_column, axis=1).set_index(index_column).T

        data_snapshot[output_column] = data_snapshot[input_columns].apply(f'{self.aggregate_function}', axis=1)
        if columns_to_drop:
            data_snapshot = data_snapshot.drop(columns_to_drop, axis=1)

        if apply_transposition:
            data_snapshot = data_snapshot.T.reset_index()
            data_snapshot.insert(loc=0, column=group_column, value=group_column_value)
            data_snapshot[index_column] = data_snapshot[index_column].apply(int)
            data_snapshot = data_snapshot.sort_values(by=index_column)

        return data_snapshot

    @staticmethod
    def __get_index_group_column(columns: Iterable[str]) -> Tuple[Optional[str], Optional[str]]:
        index_column, group_column = None, None
        for column in columns:
            if column in TimeShiftTransformer._INDEX_COLUMNS:
                index_column = column
            if column in TimeShiftTransformer._GROUP_COLUMNS:
                group_column = column

        return index_column, group_column
