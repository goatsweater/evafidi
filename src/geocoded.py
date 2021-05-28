import os
from dotenv import load_dotenv
import requests
import pandas as pd


def get_interpolated_position(geocoder_url: str, addr: str):
    payload = {'q': addr}

    # Look up the address using the geocoder
    resp = requests.get(geocoder_url, params=payload)

    # Iterate each returned object until an interpolated point is found (usually the first one)
    for result in resp.json():
        if result['qualifier'] == 'INTERPOLATED_POSITION':
            return result['geometry']['coordinates']
    
    # Nothing found. Return null island
    return [0, 0]

def add_coordinates(df):
    """Add a lon & lat column to the DataFrame."""
    coder_url = os.environ['GEOCODER_URL']
    coords = df['Address_Clean'].apply(lambda addr: get_interpolated_position(coder_url, addr))
    df = df.copy()
    df['lon'], df['lat'] = zip(*coords)
    return df

def extract_prcode(df):
    df = df.copy()
    df['prcode'] = df['Address'].str.extract(r",\s([A-Z]{2})", expand=False)
    return df

def extract_stn_count(df):
    df = df.copy()
    df['stn_count'] = df['Address'].str.extract(r"\((\d+)\sstations\)", expand=False)
    return df

def make_clean_address(df):
    df = df.copy()
    df['Address_Clean'] = df['Address'].str.replace(r"\(\d+\sstations\)", "", regex=True).str.strip()
    return df

if __name__ == '__main__':
    # Prime the environment
    load_dotenv()

    s3_opts = {
            'anon':False,
            'use_ssl': os.environ['S3_USE_SSL'],
            'client_kwargs':{
                'endpoint_url': os.environ['S3_URL']
                }
            }
    
    print("Loading and processing data...")
    s3_investments = "s3://evafidi/investments.parquet"
    df = (pd.read_parquet(s3_investments, storage_options=s3_opts)
        .pipe(extract_prcode)
        .pipe(extract_stn_count)
        .pipe(make_clean_address)
        .pipe(add_coordinates))

    print("Saving data...")
    s3_addresses = "s3://evafidi/addresses.parquet"
    df.to_parquet(s3_addresses, storage_options=s3_opts)
