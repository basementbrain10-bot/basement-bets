
import pandas as pd
import os

def inspect_excel():
    path = "data/imports/bets_combined_export.xlsx"
    if not os.path.exists(path):
        print(f"File not found: {path}")
        return

    try:
        df = pd.read_excel(path)
        print("Columns:", df.columns.tolist())
        print(f"Total Rows: {len(df)}")
        
        # Check for Date column
        # Look for 'Date' or 'Placed'
        date_cols = [c for c in df.columns if 'date' in c.lower() or 'placed' in c.lower()]
        
        if date_cols:
            print(f"Date Columns: {date_cols}")
            # Sort by first date col
            df[date_cols[0]] = pd.to_datetime(df[date_cols[0]], errors='coerce')
            df.sort_values(by=date_cols[0], inplace=True)
            print("\nLast 10 entries:")
            print(df.tail(10))
        else:
            print("\nHead of file:")
            print(df.head())
            
    except Exception as e:
        print(f"Error reading excel: {e}")

if __name__ == "__main__":
    inspect_excel()
