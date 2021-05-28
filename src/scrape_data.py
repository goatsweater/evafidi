import os

from dotenv import load_dotenv
import pandas as pd

# Scrape data from the NRCan program page to find out about investments

def read_dataset(url):
    # The table with the data is the first table on the page
    df = pd.read_html(url)[0]
    df.columns = df.columns.str.replace(' ', '_')
    df = (df.pipe(numeric_funding)
        .pipe(extract_phase)
        .pipe(clean_status_values)
        .pipe(extract_project_type)
        .assign(Province=lambda x: pd.Categorical(x['Province']),
                Status=lambda x: pd.Categorical(x['Status']),
                NRCan_Funding_Program=lambda x: pd.Categorical(x['NRCan_Funding_Program'])))
    return df

def numeric_funding(df):
    df = df.copy()
    df['NRCan_Funding'] = (pd.to_numeric(df['NRCan_Funding']
                            .str.replace('$', '', regex=False)
                            .str.replace(',', '', regex=False)))
    return df

def extract_phase(df):
    df = df.copy()
    phase = df['NRCan_Funding_Program'].str.extract('(\d)', expand=False)
    df['Phase'] = pd.to_numeric(phase)
    return df

def clean_status_values(df):
    df = df.copy()
    df['Status'] = df['Status'].str.casefold()
    return df

def extract_project_type(df):
    df = df.copy()
    types = df['Project'].str.extract("\d\s(\w+)")
    df['Project_Type'] = types
    return df


if __name__ == '__main__':
    # Prime the environment
    load_dotenv()

    phase1 = read_dataset(os.environ['PHASE1_URL'])
    phase2 = read_dataset(os.environ['PHASE2_URL'])
    investments = pd.concat([phase1, phase2])

    # Write the data back to storage
    s3_opts = {
        'anon':False,
        'use_ssl': os.environ['S3_USE_SSL'],
        'client_kwargs':{
            'endpoint_url': os.environ['S3_URL']
            }
        }
    s3_path = "s3://evafidi/investments.parquet"
    investments.to_parquet(s3_path, storage_options=opts)
