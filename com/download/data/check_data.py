import os
import pandas as pd

def check_market_data_order_and_duplicates(csv_path):
    # Read the CSV file
    df = pd.read_csv(csv_path)

    # Check if the 'open_time' and 'close_time' columns are sorted in ascending order
    is_open_time_sorted = all(df['open_time'].iloc[i] <= df['open_time'].iloc[i + 1] for i in range(len(df) - 1))
    is_close_time_sorted = all(df['close_time'].iloc[i] <= df['close_time'].iloc[i + 1] for i in range(len(df) - 1))

    # Check for duplicates
    duplicates = df.duplicated()
    has_duplicates = any(duplicates)

    return is_open_time_sorted, is_close_time_sorted, has_duplicates

if __name__ == "__main__":
    csv_path = "combined_data_1h.csv"  # Replace with the path to your CSV file
    print(os.getcwd())
    is_open_time_sorted, is_close_time_sorted, has_duplicates = check_market_data_order_and_duplicates(csv_path)

    print(f"Is open_time sorted? {is_open_time_sorted}")
    print(f"Is close_time sorted? {is_close_time_sorted}")
    print(f"Has duplicates? {has_duplicates}")
