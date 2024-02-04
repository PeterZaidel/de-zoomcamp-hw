from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
import os
from os import path
import pyarrow as pa
import pyarrow.parquet as pq

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/gcp-de-zoomcamp-ny-rides-keys.json"

@data_exporter
def export_data_to_google_cloud_storage(data: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    config = ConfigFileLoader(config_path, config_profile)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config['GOOGLE_SERVICE_ACC_KEY_FILEPATH']

    bucket_name = 'mage-zoomcamp-data'
    object_key = 'green_taxi_object_key'
    table_name = 'green_taxi'

    root = f'{bucket_name}/{table_name}'

    table = pa.Table.from_pandas(data)
    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )