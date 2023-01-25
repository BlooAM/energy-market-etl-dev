import pandas as pd

from energy_market_etl.transformers.sort.sort_transformer import SortTransformer


def test_transform():
    raw_data = pd.DataFrame({
        'A': [1, 2, 3, 4, 5],
        'B': [10, 9, 8, 7, 6],
        'C': ['10', '9', '9', '10', '10'],
    })
    transformed_data = SortTransformer(sort_by_columns=['C', 'B']).transform(raw_data)

    assert transformed_data['A'].iloc[0] == 5
    assert transformed_data['A'].iloc[-1] == 2
