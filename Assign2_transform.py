import s3fs
from s3fs.core import S3FileSystem
import numpy as np
import pickle

def transform_data():

    s3 = S3FileSystem()
    # S3 bucket directory (data lake)
    DIR = 's3://ece5984-s3-tenag/Assign2/Data_Lake2'    # Insert your S3 bucket address here. Read from the directory you created in batch ingest: Lab2/batch_ingest/
    # Get data from S3 bucket as a pickle file
    raw_data = np.load(s3.open('{}/{}'.format(DIR, 'Assign2_batch_ingest.pkl')), allow_pickle=True)


    #raw_data = np.load('data.pkl', allow_pickle=True)
    # Dividing the raw dataset for each company individual company
    raw_data.columns = raw_data.columns.swaplevel(0,1)
    raw_data.sort_index(axis=1, level=0, inplace=True)
    df_msft_rw = raw_data['MSFT']
    df_meta_rw = raw_data['META']
    df_nflx_rw = raw_data['NFLX']

    # Dropping rows with NaN in them
    df_msft = df_msft_rw.dropna()
    df_meta = df_meta_rw.dropna()
    df_nflx = df_nflx_rw.dropna()


    # Removing rows with outliers
    for col in list(df_msft.columns)[0:4]:                                          # We ignore 'Volume' column
        df_msft = df_msft.drop(df_msft[df_msft[col].values > 900].index)            # Values above 900 are dropped
        df_msft = df_msft.drop(df_msft[df_msft[col].values < 0.001].index)          # Values below 0.001 are dropped

        df_meta = df_meta.drop(df_meta[df_meta[col].values > 900].index)
        df_meta = df_meta.drop(df_meta[df_meta[col].values < 0.001].index)

        df_nflx = df_nflx.drop(df_nflx[df_nflx[col].values > 900].index)
        df_nflx = df_nflx.drop(df_nflx[df_nflx[col].values < 0.001].index)

    # Dropping duplicate rows
    df_msft = df_msft.drop_duplicates()
    df_meta = df_meta.drop_duplicates()
    df_nflx = df_nflx.drop_duplicates()

    # Push cleaned data to S3 bucket warehouse
    DIR_wh = 's3://ece5984-s3-tenag/Assign2/Data_Warehouse2'   # Insert your S3 bucket address here. Create a directory as: Lab2/transformed/
    with s3.open('{}/{}'.format(DIR_wh, 'clean_msft.pkl'), 'wb') as f:
        f.write(pickle.dumps(df_msft))
    with s3.open('{}/{}'.format(DIR_wh, 'clean_meta.pkl'), 'wb') as f:
        f.write(pickle.dumps(df_meta))
    with s3.open('{}/{}'.format(DIR_wh, 'clean_nflx.pkl'), 'wb') as f:
        f.write(pickle.dumps(df_nflx))



