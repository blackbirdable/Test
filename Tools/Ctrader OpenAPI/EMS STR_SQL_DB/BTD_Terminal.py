import pandas as pd
import time
from rich.console import Console
from rich.table import Table
from datetime import datetime
import json
import os
import traceback
import logging
import ctypes
import sqlite3

# Initialize console and logging
console = Console()
logging.basicConfig(filename='error_log.txt', level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# File paths
pnl_log_file_path = r"C:\Users\Administrator\Desktop\Sonixen\Logs\Pnl_Log_EMS_BTD_AC2.csv"
weights_file_path = r"C:\Users\Administrator\Desktop\Sonixen\Ensemble Trading\Layer2_PortfolioWeights\Final Weights\INPUT WEIGHTS.csv"
equal_weights_file_path = r"C:\Users\Administrator\Desktop\Sonixen\Ensemble Trading\Layer2_PortfolioWeights\Final Weights\EQUAL WEIGHTS.csv"
mm_weights_file_path = r"C:\Users\Administrator\Desktop\Sonixen\Ensemble Trading\Layer2_PortfolioWeights\Final Weights\MM_WEIGHTS.csv"
btc_weights_file_path = r"C:\Users\Administrator\Desktop\Sonixen\Ensemble Trading\Layer2_PortfolioWeights\Final Weights\BTC INPUT WEIGHTS.csv"
output_pnl_log_path = r"C:\Users\Administrator\Desktop\Sonixen\Tools\Ctrader OpenAPI\EMS STR_SQL_DB\Pnl_Log.py"
db_path = r"C:\Users\Administrator\Desktop\TradingData_STR_AC2.db"

def set_terminal_title(title):
    ctypes.windll.kernel32.SetConsoleTitleW(title)

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def initialize_database():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tables only if they don't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Timestamp TEXT NOT NULL,
            Type TEXT,
            Label TEXT,
            Symbol TEXT,
            Volume TEXT,
            EntryPrice TEXT,
            CurrentPrice TEXT,
            OpenPNL REAL,
            ClosedPNL REAL,
            Total REAL,
            TotalWeighted REAL,
            EqualWeighted REAL,
            MMWeighted REAL,
            BTCWeighted REAL
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS OpenOrders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Timestamp TEXT NOT NULL,
            Type TEXT,
            Label TEXT,
            Symbol TEXT,
            Volume TEXT,
            EntryPrice TEXT,
            CurrentPrice TEXT,
            OpenPNL REAL,
            ClosedPNL REAL
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Timestamp TEXT NOT NULL,
            TotalOpenPNL REAL,
            TotalClosedPNL REAL,
            TotalTotal REAL,
            TotalWeighted REAL,
            TotalEqualWeighted REAL,
            TotalMMWeighted REAL,
            BTCWeighted REAL,
            WeightedSharePrice REAL,
            EqualSharePrice REAL,
            MMSharePrice REAL,
            BTCSharePrice REAL
        )
    ''')
    
    # Check if CurrentSL and CurrentTP columns exist in Positions table
    check_positions = cursor.execute("PRAGMA table_info(Positions)").fetchall()
    column_names = [column[1] for column in check_positions]
    
    if 'CurrentSL' not in column_names:
        cursor.execute('''
            ALTER TABLE Positions ADD COLUMN CurrentSL TEXT
        ''')
    
    if 'CurrentTP' not in column_names:
        cursor.execute('''
            ALTER TABLE Positions ADD COLUMN CurrentTP TEXT
        ''')
    
    # Check if CurrentSL and CurrentTP columns exist in OpenOrders table
    check_orders = cursor.execute("PRAGMA table_info(OpenOrders)").fetchall()
    column_names = [column[1] for column in check_orders]
    
    if 'CurrentSL' not in column_names:
        cursor.execute('''
            ALTER TABLE OpenOrders ADD COLUMN CurrentSL TEXT
        ''')
    
    if 'CurrentTP' not in column_names:
        cursor.execute('''
            ALTER TABLE OpenOrders ADD COLUMN CurrentTP TEXT
        ''')
    
    conn.commit()
    conn.close()

def read_and_process_csv(pnl_log_file_path, weights_file_path, equal_weights_file_path, mm_weights_file_path, btc_weights_file_path):
    try:
        # Check if files exist
        for file_path in [pnl_log_file_path, weights_file_path, equal_weights_file_path, mm_weights_file_path, btc_weights_file_path]:
            if not os.path.exists(file_path):
                console.print(f"File not found: {file_path}", style="bold red")
                return pd.DataFrame(), pd.DataFrame()
        
        # Read the CSV files
        df = pd.read_csv(pnl_log_file_path)
        weights_df = pd.read_csv(weights_file_path)
        equal_weights_df = pd.read_csv(equal_weights_file_path)
        mm_weights_df = pd.read_csv(mm_weights_file_path)
        btc_weights_df = pd.read_csv(btc_weights_file_path)

        # Replace NaN values with empty strings
        df = df.replace(pd.NA, '', regex=True)

        # Rename columns
        df.rename(columns={
            'UnrealizedPnL': 'OpenPNL',
            'RealizedPnL': 'ClosedPNL'
        }, inplace=True)

        # Treat "Historical Trade" as "Position"
        df['Type'] = df['Type'].replace('Historical Trade', 'Position')

        # Separate Positions and Open Orders
        positions_df = df[df['Type'] == 'Position'].copy()
        open_orders_df = df[df['Type'] == 'Open Orders'].copy()

        # Filter out "TEST" and "MYBOT" labels from positions_df
        positions_df = positions_df[~positions_df['Label'].str.contains('TEST|MYBOT', case=False, na=False)]

        # Merge positions_df with weights_df, equal_weights_df, mm_weights_df, and btc_weights_df on "Label"
        positions_df = positions_df.merge(weights_df, left_on='Label', right_on='Strategy', how='left')
        positions_df = positions_df.merge(equal_weights_df, left_on='Label', right_on='Strategy', how='left', suffixes=('', '_equal'))
        positions_df = positions_df.merge(mm_weights_df, left_on='Label', right_on='Strategy', how='left', suffixes=('', '_MM'))
        positions_df = positions_df.merge(btc_weights_df, left_on='Label', right_on='Strategy', how='left', suffixes=('', '_btc'))

        # Calculate Total PnL for Positions
        positions_df['Total'] = positions_df['OpenPNL'] + positions_df['ClosedPNL']

        # Calculate TotalWeighted, EqualWeighted, MMWeighted, and BTCWeighted
        positions_df['TotalWeighted'] = positions_df['Total'] * positions_df['Weight']
        positions_df['EqualWeighted'] = positions_df['Total'] * positions_df['Weight_equal']
        positions_df['MMWeighted'] = positions_df['Total'] * positions_df['Weight_MM']
        positions_df['BTCWeighted'] = positions_df['Total'] * positions_df['Weight_btc']

        # Sort both DataFrames by Symbol
        positions_df = positions_df.sort_values('Symbol')
        open_orders_df = open_orders_df.sort_values('Symbol')

        return positions_df, open_orders_df
    except pd.errors.EmptyDataError:
        console.print(f"One of the CSV files is empty", style="bold red")
        return pd.DataFrame(), pd.DataFrame()
    except pd.errors.ParserError:
        console.print(f"Error parsing one of the CSV files - check file format", style="bold red")
        return pd.DataFrame(), pd.DataFrame()
    except KeyError as e:
        console.print(f"Missing column in one of the CSV files: {e}", style="bold red")
        return pd.DataFrame(), pd.DataFrame()
    except Exception as e:
        console.print(f"An error occurred while processing the CSV files: {e}", style="bold red")
        logging.error(f"CSV processing error: {e}\n{traceback.format_exc()}")
        return pd.DataFrame(), pd.DataFrame()

def format_volume(volume, symbol):
    if volume == '':
        return ''
    volume = float(volume)
    if symbol == 'BTCUSD':
        return f"{volume:.2f}"
    if abs(volume) >= 1000:
        return f"{volume/1000:.1f}k"
    return f"{volume:.0f}"

def format_entry_price(price):
    if price == '':
        return ''
    return f"{float(price):.6f}".rstrip('0').rstrip('.')

def create_table(df, title, share_prices=None):
    table = Table(title=title)
    headers = ["Type", "Label", "Symbol", "Volume", "EntryPrice", "CurrentPrice", "CurrentSL", "CurrentTP", "OpenPNL", "ClosedPNL", "Total", "TotalWeighted", "EqualWeighted", "MMWeighted", "BTCWeighted"]
    for header in headers:
        table.add_column(header, justify="right")

    # Check if DataFrame is empty
    if df.empty:
        table.add_row("No data available", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
        return table

    for index, row in df.iterrows():
        # Format weighted values
        def format_weighted_value(value):
            if pd.isna(value) or value == '' or value == 0:
                return f"{0:.2f}" if value == 0 else ''
            return f"[bold green]{value:.2f}[/]" if value > 0 else f"[bold red]{value:.2f}[/]"

        row_data = [
            str(row['Type']),
            str(row['Label']),
            str(row['Symbol']),
            format_volume(row['Volume'], row['Symbol']),
            f"{format_entry_price(row['EntryPrice'])}" if row['EntryPrice'] != '' else '',
            f"{row['CurrentPrice']}" if 'CurrentPrice' in row and row['CurrentPrice'] != '' else '',
            f"{row['CurrentSL']}" if 'CurrentSL' in row and row['CurrentSL'] != '' and row['CurrentSL'] != 0 else '',
            f"{row['CurrentTP']}" if 'CurrentTP' in row and row['CurrentTP'] != '' and row['CurrentTP'] != 0 else '',
            (f"[bold green]{row['OpenPNL']:.2f}[/]" if row['OpenPNL'] > 0 else f"{row['OpenPNL']:.2f}" if row['OpenPNL'] == 0 else f"[bold red]{row['OpenPNL']:.2f}[/]") if row['OpenPNL'] != '' else '',
            f"{row['ClosedPNL']:.2f}" if row['ClosedPNL'] != '' else '',
            (f"[bold green]{row['Total']:.2f}[/]" if row['Total'] > 0 else f"{row['Total']:.2f}" if row['Total'] == 0 else f"[bold red]{row['Total']:.2f}[/]") if 'Total' in row and row['Total'] != '' else '',
            format_weighted_value(row['TotalWeighted']) if 'TotalWeighted' in row else '',
            format_weighted_value(row['EqualWeighted']) if 'EqualWeighted' in row else '',
            format_weighted_value(row['MMWeighted']) if 'MMWeighted' in row else '',
            format_weighted_value(row['BTCWeighted']) if 'BTCWeighted' in row else ''
        ]
        table.add_row(*row_data)

    if title == "Positions" and not df.empty and 'OpenPNL' in df.columns:
        total_open_pnl = df['OpenPNL'].sum()
        total_closed_pnl = df['ClosedPNL'].sum()
        total_total = df['Total'].sum()
        total_weighted = df['TotalWeighted'].sum()
        total_equal_weighted = df['EqualWeighted'].sum()
        total_MM_weighted = df['MMWeighted'].sum()
        total_btc_weighted = df['BTCWeighted'].sum()

        share_price_weighted = (total_weighted + 5000) / 50
        share_price_equal = (total_equal_weighted + 5000) / 50
        share_price_MM = (total_MM_weighted + 10000) / 100
        share_price_btc = (total_btc_weighted + 3000) / 30

        if share_prices is not None:
            share_prices.update({
                'weighted': share_price_weighted,
                'equal': share_price_equal,
                'MM': share_price_MM,
                'btc': share_price_btc
            })

        # Format totals weighted values
        def format_total_weighted(value):
            if pd.isna(value) or value == 0:
                return f"{0:.2f}"
            return f"[bold green]{value:.2f}[/]" if value > 0 else f"[bold red]{value:.2f}[/]"

        table.add_row(
            "Total", "", "", "", "", "", "", "", 
            (f"[bold green]{total_open_pnl:.2f}[/]" if total_open_pnl > 0 else f"{total_open_pnl:.2f}" if total_open_pnl == 0 else f"[bold red]{total_open_pnl:.2f}[/]"),
            f"{total_closed_pnl:.2f}", 
            (f"[bold green]{total_total:.2f}[/]" if total_total > 0 else f"{total_total:.2f}" if total_total == 0 else f"[bold red]{total_total:.2f}[/]"),
            format_total_weighted(total_weighted),
            format_total_weighted(total_equal_weighted),
            format_total_weighted(total_MM_weighted),
            format_total_weighted(total_btc_weighted)
        )

        table.add_row(
            "Share Price", "", "", "", "", "", "", "", 
            "", "", "",
            f"[bold cyan]${share_price_weighted:.2f}[/]",
            f"[bold cyan]${share_price_equal:.2f}[/]",
            f"[bold cyan]${share_price_MM:.2f}[/]",
            f"[bold cyan]${share_price_btc:.2f}[/]"
        )

    return table

def save_data_to_db(positions_df, open_orders_df, share_prices):
   timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
   
   # If both DataFrames are empty, don't save anything
   if positions_df.empty and open_orders_df.empty:
       console.print("No data to save to database", style="bold yellow")
       return
   
   conn = sqlite3.connect(db_path)
   cursor = conn.cursor()
   
   try:
       # Insert Positions data
       if not positions_df.empty and 'OpenPNL' in positions_df.columns:
           for _, row in positions_df.iterrows():
               cursor.execute('''
                   INSERT INTO Positions (
                       Timestamp, Type, Label, Symbol, Volume, EntryPrice, 
                       CurrentPrice, CurrentSL, CurrentTP, OpenPNL, ClosedPNL, Total, TotalWeighted, 
                       EqualWeighted, MMWeighted, BTCWeighted
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ''', (
                   timestamp, row['Type'], row['Label'], row['Symbol'],
                   str(row['Volume']), str(row['EntryPrice']), str(row['CurrentPrice']),
                   str(row['CurrentSL']), str(row['CurrentTP']), 
                   row['OpenPNL'], row['ClosedPNL'], row['Total'],
                   row['TotalWeighted'], row['EqualWeighted'], row['MMWeighted'], row['BTCWeighted']
               ))
       
       # Insert OpenOrders data
       if not open_orders_df.empty:
           for _, row in open_orders_df.iterrows():
               cursor.execute('''
                   INSERT INTO OpenOrders (
                       Timestamp, Type, Label, Symbol, Volume, EntryPrice,
                       CurrentPrice, CurrentSL, CurrentTP, OpenPNL, ClosedPNL
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ''', (
                   timestamp, row['Type'], row['Label'], row['Symbol'],
                   str(row['Volume']), str(row['EntryPrice']), str(row['CurrentPrice']),
                   str(row['CurrentSL']), str(row['CurrentTP']),
                   row['OpenPNL'], row['ClosedPNL']
               ))
       
       # Insert Summary data only if positions_df has data
       if not positions_df.empty and 'OpenPNL' in positions_df.columns:
           cursor.execute('''
               INSERT INTO Summary (
                   Timestamp, TotalOpenPNL, TotalClosedPNL, TotalTotal,
                   TotalWeighted, TotalEqualWeighted, TotalMMWeighted, BTCWeighted,
                   WeightedSharePrice, EqualSharePrice, MMSharePrice, BTCSharePrice
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ''', (
               timestamp,
               positions_df['OpenPNL'].sum(),
               positions_df['ClosedPNL'].sum(),
               positions_df['Total'].sum(),
               positions_df['TotalWeighted'].sum(),
               positions_df['EqualWeighted'].sum(),
               positions_df['MMWeighted'].sum(),
               positions_df['BTCWeighted'].sum(),
               share_prices.get('weighted', 0),
               share_prices.get('equal', 0),
               share_prices.get('MM', 0),
               share_prices.get('btc', 0)
           ))
       
       conn.commit()
   except Exception as e:
       conn.rollback()
       console.print(f"Database error: {e}", style="bold red")
       raise e
   finally:
       conn.close()

def save_pnl_log(positions_df, open_orders_df, output_path):
    # Create a copy of positions_df
    positions_filtered = positions_df.copy()
    
    # Check if 'Volume' column exists before filtering
    if 'Volume' in positions_filtered.columns:
        # Convert empty strings to '0' before conversion to float
        positions_filtered['Volume'] = positions_filtered['Volume'].replace('', '0')
        # Filter out positions with 0 volume
        positions_filtered = positions_filtered[positions_filtered['Volume'].astype(float) != 0]
    
    # For positions, only include the specified fields
    positions_list = []
    for _, row in positions_filtered.iterrows():
        position = {
            "Type": "Position",
            "Label": row.get('Label', ''),
            "Symbol": row.get('Symbol', ''),
            "Volume": float(row.get('Volume', 0)) if row.get('Volume', '') != '' else 0,
            "EntryPrice": float(row.get('EntryPrice', 0)) if row.get('EntryPrice', '') != '' else 0,
            "CurrentPrice": float(row.get('CurrentPrice', 0)) if row.get('CurrentPrice', '') != '' else 0,
            "CurrentSL": float(row.get('CurrentSL', 0)) if row.get('CurrentSL', '') != '' else 0,
            "CurrentTP": float(row.get('CurrentTP', 0)) if row.get('CurrentTP', '') != '' else 0,
            "OpenPNL": float(row.get('OpenPNL', 0)),
            "ClosedPNL": float(row.get('ClosedPNL', 0)),
            "Total": float(row.get('Total', 0))
        }
        positions_list.append(position)
    
    combined_data = {
        'positions': positions_list,
        'open_orders': open_orders_df.to_dict(orient='records')
    }
    with open(output_path, 'w') as f:
        f.write(f"pnl_data = {json.dumps(combined_data, indent=2)}\n")

def main():
   set_terminal_title("Terminal")
   console.print("Loading data...", style="bold cyan")
   
   # Initialize database when starting
   try:
       initialize_database()
       console.print("Database initialized successfully", style="bold green")
   except Exception as e:
       console.print(f"Error initializing database: {e}", style="bold red")
       return

   time.sleep(5)

   while True:
       try:
           clear_screen()

           positions_df, open_orders_df = read_and_process_csv(
               pnl_log_file_path, 
               weights_file_path, 
               equal_weights_file_path, 
               mm_weights_file_path, 
               btc_weights_file_path
           )

           share_prices = {}

           positions_table = create_table(positions_df, "Positions", share_prices)
           open_orders_table = create_table(open_orders_df, "Open Orders")

           console.print(positions_table)
           console.print(open_orders_table)

           save_pnl_log(positions_df, open_orders_df, output_pnl_log_path)

           save_data_to_db(positions_df, open_orders_df, share_prices)

       except Exception as e:
           error_msg = f"An error occurred: {str(e)}\n{traceback.format_exc()}"
           logging.error(error_msg)
           console.print(f"An error occurred. Check error_log.txt for details.", style="bold red")

       finally:
           # Wait for 60 seconds before refreshing
           time.sleep(60)

if __name__ == "__main__":
   main()
