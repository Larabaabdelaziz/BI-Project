import pandas as pd
import os

def check_csv_structure():
    files = ['Customers.csv', 'Products.csv', 'Orders.csv', 'Order Details.csv']
    base_path = 'data/SQLserver/csv/'
    
    for file in files:
        path = os.path.join(base_path, file)
        if os.path.exists(path):
            df = pd.read_csv(path, encoding='latin1')
            print(f"\n{file}:")
            print(f"  Columns: {list(df.columns)}")
            print(f"  First row: {df.iloc[0].to_dict() if len(df) > 0 else 'Empty'}")
        else:
            print(f"\n{file}: File not found at {path}")

check_csv_structure()