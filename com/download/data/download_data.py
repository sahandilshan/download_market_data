import requests
import zipfile
import os
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime


def fetch_and_combine_data(url, market_name, start_date, end_date):
    # Fetch XML response from the S3 URL
    response = requests.get(url)
    root = ET.fromstring(response.content)
    namespace = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
    data_type = url.split('/')[-2]

    # Create necessary folders
    download_folder = f"downloaded_zips/{market_name}/{data_type}"
    extract_folder = f"extracted_csvs/{market_name}/{data_type}"

    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
    if not os.path.exists(extract_folder):
        os.makedirs(extract_folder)

    # Get ZIP files from XML response
    zip_files = []
    for content in root.findall("s3:Contents", namespace):
        key = content.find("s3:Key", namespace).text
        if key.endswith(".zip"):
            date_str = '-'.join(key.split('-')[2:5]).replace('.zip', '')
            zip_date = datetime.strptime(date_str, '%Y-%m-%d')
            if start_date <= zip_date <= end_date:
                zip_files.append(key)

    # Download ZIP files
    for zip_file in sorted(zip_files):
        zip_url = f'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/{zip_file}'
        local_zip_path = os.path.join(download_folder, zip_file.split('/')[-1])
        print(f"Downloading {zip_url} to {local_zip_path}")

        if not os.path.exists(local_zip_path):
            response = requests.get(zip_url)
            with open(local_zip_path, 'wb') as f:
                f.write(response.content)
                print(f"Downloaded {zip_file}")
        else:
            print(f"File {zip_file} already exists. Skipping download.")

    # Extract ZIP files
    for local_zip_path in sorted(os.listdir(download_folder)):
        file_name = local_zip_path
        date_str = '-'.join(file_name.split('-')[2:5]).replace('.zip', '')
        zip_date = datetime.strptime(date_str, '%Y-%m-%d')

        extracted_csv_name = f"{date_str}.csv"
        extracted_csv_path = os.path.join(extract_folder, extracted_csv_name)

        if start_date <= zip_date <= end_date:
            if not os.path.exists(extracted_csv_path):
                print(f"Extracting {local_zip_path}")
                with zipfile.ZipFile(os.path.join(download_folder, local_zip_path), 'r') as zip_ref:
                    zip_ref.extractall(extract_folder)
            else:
                print(f"File {local_zip_path} has already been extracted. Skipping extraction.")

    # Combine CSV files into a single CSV
    dfs = []
    column_names = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'count',
                    'taker_buy_volume', 'taker_buy_quote_volume', 'ignore']

    for csv_file in sorted(os.listdir(extract_folder)):
        csv_date_str = csv_file.split('-')[-3:]
        csv_date_str = '-'.join(csv_date_str).replace('.csv', '')
        csv_date = datetime.strptime(csv_date_str, '%Y-%m-%d')

        if start_date <= csv_date <= end_date:
            csv_path = os.path.join(extract_folder, csv_file)

            # Try reading the CSV with headers first
            try:
                df = pd.read_csv(csv_path)
                if list(df.columns) != list(column_names):
                    raise ValueError('Invalid format')
            except:
                # Fallback to reading without headers
                df = pd.read_csv(csv_path, header=None, names=column_names)

            dfs.append(df)

    if dfs:
        final_df = pd.concat(dfs, ignore_index=True)
        final_csv_path = f"combined_data_{data_type}.csv"
        final_df.to_csv(final_csv_path, index=False)
        print(f"Combined data saved to {final_csv_path}")
    else:
        print("No DataFrames to concatenate.")


if __name__ == "__main__":

    time_interval = '1h'
    market = 'ETHUSDT'
    s3_url = f"https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/daily/" \
             f"klines/{market}/{time_interval}/&marker=data%2Ffutures%2Fum%2Fdaily%2Fklines%2F{market}%2F" \
             f"{time_interval}%2F{market}-{time_interval}-2021-12-31.zip.CHECKSUM"

    start_date = datetime.strptime('2022-01-01', '%Y-%m-%d')
    end_date = datetime.strptime('2023-08-31', '%Y-%m-%d')

    fetch_and_combine_data(s3_url, market, start_date, end_date)
