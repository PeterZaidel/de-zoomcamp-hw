import re
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    num_zero_passengers = data['passenger_count'].isin([0]).sum()
    print(f'Preprocessing: rows with zero passengers: {num_zero_passengers}')
    print(f'Rows before filter: {data.shape[0]}')
    data = data[
        (data['passenger_count'] > 0)
        & (data['trip_distance'] > 0)
    ]
    print(f'Rows after filter: {data.shape[0]}')
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # rename columns
    columns = data.columns
    renamed_columns = [
        re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
        for name in columns
    ]
    rename_dict = {
        name: snake_name for name, snake_name in zip(columns, renamed_columns)
    }
    print('Renamed columns: ', [x for x in rename_dict.items() if x[0] != x[1]])
    rename_dict['VendorID'] = 'vendor_id'
    data = data.rename(columns=rename_dict)
    print(
        "Uniq VendorId values: ", 
        list(data['vendor_id'].unique())
    )
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
