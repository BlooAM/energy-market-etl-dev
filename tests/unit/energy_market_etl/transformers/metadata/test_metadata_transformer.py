import pandas as pd

from energy_market_etl.transformers.metadata.metadata_transformer import MetadataTransformer


def test_transform():
    raw_data = pd.DataFrame(
        {
        ' A': [1, 2, 3],
        'B  ': [4, 5, 6],
        '\nC\t': ['7', '8', '9'],
        },
        index=[3, 2, 1]
    )
    transformed_data = MetadataTransformer(reset_index=True).transform(raw_data)

    assert set(transformed_data.columns) == {'A', 'B', 'C'}
    assert transformed_data.index.to_list() == [0, 1, 2]
