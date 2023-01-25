import datetime as dt
from typing import Callable, Dict, List, Union

import pandas as pd

from energy_market_etl.transformers.transformer import Transformer
from energy_market_etl.utils.date_utils import get_march_switch, get_october_switch


class TimeShiftTransformer(Transformer):
    def __init__(
            self,
            input_columns: List[str],
            output_column: str,
            aggregate_function: Union[str, Callable] = 'mean',
            index_column: str = 'Godzina',
            group_column: str = 'Data',
    ) -> None:
        self.input_columns = input_columns
        self.output_column = output_column
        self.aggregate_function = aggregate_function
        self.index_column = index_column
        self.group_column = group_column

    def transform(self, raw_data_snapshots: Dict[dt.datetime, pd.DataFrame]) -> Dict[dt.datetime, pd.DataFrame]:
        data_snapshots = {}
        for datetime, raw_data_snapshot in raw_data_snapshots.items():
            current_date = datetime.date()
            current_year = datetime.year
            apply_transposition = False if set(self.input_columns) <= set(raw_data_snapshot.columns) else True

            if current_date == get_march_switch(year=current_year).date():
                data_snapshots[datetime] = self.__apply_march_shift(raw_data_snapshot, apply_transposition)
            elif current_date == get_october_switch(year=current_year).date():
                data_snapshots[datetime] = self.__apply_october_shift(raw_data_snapshot, apply_transposition)
            else:
                print('No data shift...')

        return data_snapshots

    def __apply_march_shift(self, data_snapshot: pd.DataFrame, apply_transposition: bool) -> pd.DataFrame:

        transformed_data_snapshot = data_snapshot.copy()
        # transformed_data_snapshot[base_column] = \
        #     transformed_data_snapshot[[base_column, time_shif_column]].apply(f'{aggregate_function}', axis=1)
        # transformed_data_snapshot = transformed_data_snapshot.drop(time_shif_column, axis=1)

        # df1_t = df1.drop('Data', axis=1).set_index('Godzina').T
        #
        # df1_t['2'] = df1_t[['2', '2A']].apply('mean', axis=1)
        # df1_t = df1_t.drop('2A', axis=1)
        #
        # df1_t = df1_t.T.reset_index()
        # df1_t.insert(0, "Data", df1['Data'])

        return transformed_data_snapshot

    def __apply_october_shift(self, data_snapshot: pd.DataFrame, apply_transposition: bool) -> pd.DataFrame:
        print('Applying october shift...')
        transformed_data_snapshot = data_snapshot.copy()
        column_to_drop = self.input_columns[-1]

        if apply_transposition:
            transformed_data_snapshot = \
                transformed_data_snapshot.drop(self.group_column, axis=1).set_index(self.index_column).T

        transformed_data_snapshot[self.output_column] = \
            transformed_data_snapshot[self.input_columns].apply(f'{self.aggregate_function}', axis=1)
        transformed_data_snapshot = transformed_data_snapshot.drop(column_to_drop, axis=1)

        if apply_transposition:
            transformed_data_snapshot = transformed_data_snapshot.T.reset_index()
            transformed_data_snapshot.insert(
                loc=0,
                column=self.group_column,
                value=data_snapshot[self.group_column]
            )
        return transformed_data_snapshot

# df1_t = TimeShiftTransformer(input_columns=['2', '2A'], output_column='2').transform({period_end_date: df1})