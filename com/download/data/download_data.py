import requests
import zipfile
import os
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

# Define the date range
start_date = datetime.strptime('2023-01-01', '%Y-%m-%d')
end_date = datetime.strptime('2023-08-01', '%Y-%m-%d')

# Fetch XML response from the S3 URL
# s3_url = 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/daily/klines/ETHUSDT/1h/'
s3_url = 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/daily/klines' \
         '/ETHUSDT/1h/&marker=data%2Ffutures%2Fum%2Fdaily%2Fklines%2FETHUSDT%2F1h%2FETHUSDT-1h-2022-09-25.zip.CHECKSUM'
response = requests.get(s3_url)
root = ET.fromstring(response.content)

# Define the XML namespace
namespace = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}

# Parse the XML and find ZIP files
zip_files = []
for content in root.findall("s3:Contents", namespace):
    key = content.find("s3:Key", namespace).text
    if key.endswith(".zip"):
        # Extract date from ZIP file name
        file_name = key.split('/')[-1]  # Get the last part of the key
        date_str = '-'.join(file_name.split('-')[2:5]).replace('.zip', '')  # Extract the date part from the file name
        zip_date = datetime.strptime(date_str, '%Y-%m-%d')  # Convert it to a datetime object

        # Check if the date falls within the specified range
        if start_date <= zip_date <= end_date:
            zip_files.append(key)

# Download ZIP files
download_folder = "downloaded_zips"
if not os.path.exists(download_folder):
    os.makedirs(download_folder)

for zip_file in sorted(zip_files):
    zip_url = f'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/{zip_file}'
    local_zip_path = os.path.join(download_folder, zip_file.split('/')[-1])
    response = requests.get(zip_url)
    with open(local_zip_path, 'wb') as f:
        f.write(response.content)
        print(f"Downloaded {zip_file}")

# Print downloaded ZIP files
print(f"Downloaded ZIP files: {os.listdir(download_folder)}")

# Extract CSV files
extract_folder = "extracted_csvs"
if not os.path.exists(extract_folder):
    os.makedirs(extract_folder)

for local_zip_path in sorted(os.listdir(download_folder)):
    with zipfile.ZipFile(os.path.join(download_folder, local_zip_path), 'r') as zip_ref:
        zip_ref.extractall(extract_folder)

# Print extracted CSV files
print(f"Extracted CSV files: {os.listdir(extract_folder)}")

# Combine CSV files into a single CSV
csv_files = sorted([f for f in os.listdir(extract_folder) if f.endswith('.csv')])
dfs = []

# Read the first CSV to get the column names
first_csv_path = os.path.join(extract_folder, csv_files[0])
first_df = pd.read_csv(first_csv_path)
column_names = first_df.columns

for csv_file in csv_files:
    csv_path = os.path.join(extract_folder, csv_file)
    df = pd.read_csv(csv_path)

    # Check if the DataFrame has the same columns as the first one
    if list(df.columns) == list(column_names):
        dfs.append(df)
    else:
        print(f"Skipping {csv_file} due to different column structure")

# Print DataFrames to concatenate
print(f"DataFrames to concatenate: {len(dfs)}")

# Concatenate the DataFrames
if dfs:
    final_df = pd.concat(dfs, ignore_index=True)
    final_df.to_csv("combined_data.csv", index=False)
else:
    print("No DataFrames to concatenate.")
