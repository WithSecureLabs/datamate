import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import os
import json
import dask.bag as db
from datetime import datetime
from dask.distributed import Client
from pandas.core.dtypes.common import pandas_dtype as dtype
import pandas as pd

CONFIG = json.load(open("processing_config.json"))
MOUNTED_FOLDER = r"/temp"
PARQUET_FILENAME = os.environ.get("PARQUET_FILENAME")

class DaskJsonToParquet:
    TARGETED_COLS = CONFIG['targeted_columns']
    COLS_DTYPES = CONFIG['columns_dtypes']

    def __init__(self, input_folder: str, output_folder: str) -> None:
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.custom_dtypes = dict()
        self.df = pd.DataFrame()
        data_files = os.listdir(self.input_folder)

    def create_output_folder(self) -> None:
        print("Create output folder if it doesn't exist")
        os.makedirs(self.output_folder, exist_ok=True)

    @staticmethod
    def timestamp():
        return datetime.now().strftime("%d-%m-%Y, %H:%M:%S")

    def initiate_dask_local_cluster(self):
        """Initiate the local cluster with max amount of workers""" 
        print(f"Initiate local cluster with {os.cpu_count()} CPUs")
        Client(n_workers=os.cpu_count())

    def setup_dtypes(self):
        print("Setup columns dtypes")
        self.custom_dtypes = {k:dtype(v) for k, v in zip(self.TARGETED_COLS, self.COLS_DTYPES)}

    def process_data(self):
        print("Process data")
        bag = db.read_text(f'{self.input_folder}/*.json', blocksize="128MiB").map(json.loads)
        bag = bag.map(lambda x: {key: value for key, value in x.items() if key in self.TARGETED_COLS})
        self.df = bag.to_dataframe(meta=self.custom_dtypes)

    def save_to_parquet(self):
        print("Save to parquet")
        self.df.to_parquet(os.path.join(self.output_folder, PARQUET_FILENAME), engine="pyarrow")


def main():

    converter = DaskJsonToParquet(
        input_folder=MOUNTED_FOLDER,
        output_folder=f"{MOUNTED_FOLDER}/output"
    )

    print(converter.timestamp(), "Start converting")

    converter.create_output_folder()
    converter.initiate_dask_local_cluster()
    converter.setup_dtypes()
    converter.process_data()
    converter.save_to_parquet()

    print(converter.timestamp(), "End converting")


if __name__ == "__main__":
    main()
