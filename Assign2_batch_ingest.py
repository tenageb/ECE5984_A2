import s3fs
import numpy as np
import pickle
import pandas as pd
from pandas_datareader import data as pdr
import yfinance as yfin

def ingest_data():
    # Define the start date and the stock tickers to retrieve data for
    start_date = '2005-01-01'
    tickers = ["MSFT", "META", "NFLX"]

    # Override Yahoo Finance API with pandas data reader
    yfin.pdr_override()

    # Retrieve stock data and store it in a pandas DataFrame
    stock_data = pdr.get_data_yahoo(tickers, start=start_date)

    # Introduce NaN values in a small fraction of the dataset to simulate missing data
    stock_data = stock_data.apply(lambda col: col.mask(np.random.rand(len(col)) < 0.001))

    # Select a random column, other than 'Volume', to introduce outliers
    random_column = stock_data.sample(axis='columns').columns[0]
    if random_column[0] != 'Volume':
        outlier_indices = stock_data.sample(frac=0.005).index
        stock_data.loc[outlier_indices, random_column] = [1000] * len(outlier_indices)
        stock_data.loc[stock_data.sample(frac=0.005).index, random_column] = 0

    # Add duplicate records to simulate noisy dataset
    duplicate_data = stock_data.sample(frac=0.005)
    stock_data = pd.concat([stock_data, duplicate_data])

    # Initialize S3 file system and specify the S3 bucket directory
    s3 = s3fs.S3FileSystem()
    s3_dir = 's3://ece5984-s3-tenag/Assign2/Data_Lake2'  # Insert  your S3 bucket address here. Create a directory as Assign2/batch_ingest

    # Save the modified dataset to the S3 bucket as a pickle file
    with s3.open(f'{s3_dir}/Assign2_batch_ingest.pkl', 'wb') as file:
        pickle.dump(stock_data, file)

