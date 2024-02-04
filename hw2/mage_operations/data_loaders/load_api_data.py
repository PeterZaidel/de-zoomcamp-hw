import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    base_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/'
    urls = [
        base_url + 'green_tripdata_2020-10.csv.gz',
        base_url + 'green_tripdata_2020-11.csv.gz',
        base_url + 'green_tripdata_2020-12.csv.gz',
    ]

    dtypes = {
        'VendorID': pd.Int64Dtype(),
        'lpep_pickup_datetime': str,
        'lpep_dropoff_datetime': str,
        'store_and_fwd_flag': str,
        'RatecodeID': float,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'ehail_fee': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'payment_type': float,
        'trip_type': float,
        'congestion_surcharge': float,
    }

    data = []
    for url in urls:
        df = pd.read_csv(
            url,
            dtype=dtypes,
            parse_dates=['lpep_pickup_datetime', 'lpep_dropoff_datetime'],
            sep=',',
            compression='gzip'
        )
        data.append(df)
    res_df = pd.concat(data)
    return res_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
