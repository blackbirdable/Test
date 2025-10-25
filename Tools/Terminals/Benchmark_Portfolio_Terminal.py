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
from google.cloud import bigquery
from google.oauth2 import service_account

# Set pandas option to avoid future warnings
pd.set_option('future.no_silent_downcasting', True)

# Initialize console and logging
console = Console()
logging.basicConfig(filename='error_log.txt', level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# File paths
pnl_log_file_path = r"C:\Users\Administrator\Desktop\Sonixen\Logs\Pnl_Log_EMS.csv"
weights_file_path = r"C:\Users\Administrator\Desktop\Sonixen\Ensemble Trading\Layer2_PortfolioWeights\Final Weights\INPUT WEIGHTS.csv"
equal_weights_file_path = r"C:\Users\Administrator\Desktop\Sonixen\Ensemble Trading\Layer2_PortfolioWeights\Final Weights\EQUAL WEIGHTS.csv"
mm_weights_file_path = r"C:\Users\Administrator\Desktop\Sonixen\Ensemble Trading\Layer2_PortfolioWeights\Final Weights\MM_WEIGHTS.csv"
btc_weights_file_path = r"C:\Users\Administrator\Desktop\Sonixen\Ensemble Trading\Layer2_PortfolioWeights\Final Weights\BTC INPUT WEIGHTS.csv"
output_pnl_log_path = r"C:\Users\Administrator\Desktop\Pnl_Log.py"

# BigQuery configuration
credentials_path = r"C:\Users\Administrator\Desktop\Sonixen\Tools\Ctrader OpenAPI\massive-clone-400221-a39952ec190a.json"
project_id = "massive-clone-400221"  # Your existing project ID
dataset_id = "benchmark_portfolio"   # New dedicated dataset

def set_terminal_title(title):
    ctypes.windll.kernel32.SetConsoleTitleW(title)

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def initialize_bigquery_client():
    """Initialize and return a BigQuery client."""
    try:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path, 
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        client = bigquery.Client(credentials=credentials, project=project_id)
        
        # Create dataset if it doesn't exist
        try:
            client.get_dataset(dataset_id)
            console.print(f"Dataset {dataset_id} already exists", style="bold cyan")
        except Exception:
            dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
            dataset.location = "US"  # Specify the location
            dataset = client.create_dataset(dataset, timeout=30)
            console.print(f"Created dataset {dataset_id}", style="bold green")
        
        # Create tables if they don't exist
        create_bigquery_tables(client)
        
        return client
    except Exception as e:
        console.print(f"Error initializing BigQuery client: {e}", style="bold red")
        logging.error(f"BigQuery client initialization error: {e}\n{traceback.format_exc()}")
        raise

def create_bigquery_tables(client):
    """Create BigQuery tables if they don't exist."""
    # Positions table
    positions_schema = [
        bigquery.SchemaField("Timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("Type", "STRING"),
        bigquery.SchemaField("Label", "STRING"),
        bigquery.SchemaField("Symbol", "STRING"),
        bigquery.SchemaField("Volume", "STRING"),
        bigquery.SchemaField("EntryPrice", "STRING"),
        bigquery.SchemaField("CurrentPrice", "STRING"),
        bigquery.SchemaField("CurrentSL", "STRING"),
        bigquery.SchemaField("CurrentTP", "STRING"),
        bigquery.SchemaField("OpenPNL", "FLOAT"),
        bigquery.SchemaField("ClosedPNL", "FLOAT"),
        bigquery.SchemaField("Total", "FLOAT"),
        bigquery.SchemaField("TotalWeighted", "FLOAT"),
        bigquery.SchemaField("EqualWeighted", "FLOAT"),
        bigquery.SchemaField("MMWeighted", "FLOAT"),
        bigquery.SchemaField("BTCWeighted", "FLOAT")
    ]
    
    # Open Orders table
    open_orders_schema = [
        bigquery.SchemaField("Timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("Type", "STRING"),
        bigquery.SchemaField("Label", "STRING"),
        bigquery.SchemaField("Symbol", "STRING"),
        bigquery.SchemaField("Volume", "STRING"),
        bigquery.SchemaField("EntryPrice", "STRING"),
        bigquery.SchemaField("CurrentPrice", "STRING"),
        bigquery.SchemaField("CurrentSL", "STRING"),
        bigquery.SchemaField("CurrentTP", "STRING"),
        bigquery.SchemaField("OpenPNL", "FLOAT"),
        bigquery.SchemaField("ClosedPNL", "FLOAT")
    ]
    
    # Summary table
    summary_schema = [
        bigquery.SchemaField("Timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("TotalOpenPNL", "FLOAT"),
        bigquery.SchemaField("TotalClosedPNL", "FLOAT"),
        bigquery.SchemaField("TotalTotal", "FLOAT"),
        bigquery.SchemaField("TotalWeighted", "FLOAT"),
        bigquery.SchemaField("TotalEqualWeighted", "FLOAT"),
        bigquery.SchemaField("TotalMMWeighted", "FLOAT"),
        bigquery.SchemaField("BTCWeighted", "FLOAT"),
        bigquery.SchemaField("WeightedSharePrice", "FLOAT"),
        bigquery.SchemaField("EqualSharePrice", "FLOAT"),
        bigquery.SchemaField("MMSharePrice", "FLOAT"),
        bigquery.SchemaField("BTCSharePrice", "FLOAT")
    ]
    
    tables = [
        {"table_id": "Positions", "schema": positions_schema},
        {"table_id": "OpenOrders", "schema": open_orders_schema},
        {"table_id": "Summary", "schema": summary_schema}
    ]
    
    for table_info in tables:
        table_id = f"{project_id}.{dataset_id}.{table_info['table_id']}"
        try:
            client.get_table(table_id)
            console.print(f"Table {table_info['table_id']} already exists", style="bold cyan")
        except Exception:
            table = bigquery.Table(table_id, schema=table_info['schema'])
            table = client.create_table(table)
            console.print(f"Created table {table.table_id}", style="bold green")

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

def save_data_to_bigquery(bq_client, positions_df, open_orders_df, share_prices):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # If both DataFrames are empty, don't save anything
    if positions_df.empty and open_orders_df.empty:
        console.print("No data to save to BigQuery", style="bold yellow")
        return
    
    try:
        # Handle NaN values in positions_df
        if not positions_df.empty:
            # Replace NaN with None which BigQuery will interpret as NULL
            positions_df = positions_df.replace({pd.NA: None, float('nan'): None})

        # Handle NaN values in open_orders_df
        if not open_orders_df.empty:
            open_orders_df = open_orders_df.replace({pd.NA: None, float('nan'): None})
        
        # Insert Positions data
        if not positions_df.empty and 'OpenPNL' in positions_df.columns:
            positions_rows = []
            for _, row in positions_df.iterrows():
                # Create a clean row dictionary to ensure no NaN values
                clean_row = {
                    "Timestamp": timestamp,
                    "Type": row['Type'],
                    "Label": row['Label'],
                    "Symbol": row['Symbol'],
                    "Volume": str(row['Volume']) if row['Volume'] is not None else "",
                    "EntryPrice": str(row['EntryPrice']) if row['EntryPrice'] is not None else "",
                    "CurrentPrice": str(row['CurrentPrice']) if row['CurrentPrice'] is not None else "",
                    "CurrentSL": str(row['CurrentSL']) if row['CurrentSL'] is not None else "",
                    "CurrentTP": str(row['CurrentTP']) if row['CurrentTP'] is not None else "",
                    "OpenPNL": float(row['OpenPNL']) if row['OpenPNL'] is not None else 0.0,
                    "ClosedPNL": float(row['ClosedPNL']) if row['ClosedPNL'] is not None else 0.0,
                    "Total": float(row['Total']) if row['Total'] is not None else 0.0
                }
                
                # Add the weighted fields only if they exist and are not NaN
                if 'TotalWeighted' in row and row['TotalWeighted'] is not None:
                    clean_row["TotalWeighted"] = float(row['TotalWeighted'])
                else:
                    clean_row["TotalWeighted"] = 0.0
                    
                if 'EqualWeighted' in row and row['EqualWeighted'] is not None:
                    clean_row["EqualWeighted"] = float(row['EqualWeighted'])
                else:
                    clean_row["EqualWeighted"] = 0.0
                    
                if 'MMWeighted' in row and row['MMWeighted'] is not None:
                    clean_row["MMWeighted"] = float(row['MMWeighted'])
                else:
                    clean_row["MMWeighted"] = 0.0
                    
                if 'BTCWeighted' in row and row['BTCWeighted'] is not None:
                    clean_row["BTCWeighted"] = float(row['BTCWeighted'])
                else:
                    clean_row["BTCWeighted"] = 0.0
                
                positions_rows.append(clean_row)
            
            table_id = f"{project_id}.{dataset_id}.Positions"
            errors = bq_client.insert_rows_json(table_id, positions_rows)
            if errors:
                console.print(f"Errors occurred while inserting positions data: {errors}", style="bold red")
        
        # Insert OpenOrders data
        if not open_orders_df.empty:
            open_orders_rows = []
            for _, row in open_orders_df.iterrows():
                clean_row = {
                    "Timestamp": timestamp,
                    "Type": row['Type'],
                    "Label": row['Label'],
                    "Symbol": row['Symbol'],
                    "Volume": str(row['Volume']) if row['Volume'] is not None else "",
                    "EntryPrice": str(row['EntryPrice']) if row['EntryPrice'] is not None else "",
                    "CurrentPrice": str(row['CurrentPrice']) if row['CurrentPrice'] is not None else "",
                    "CurrentSL": str(row.get('CurrentSL', "")) if row.get('CurrentSL') is not None else "",
                    "CurrentTP": str(row.get('CurrentTP', "")) if row.get('CurrentTP') is not None else "",
                    "OpenPNL": float(row['OpenPNL']) if row['OpenPNL'] is not None else 0.0,
                    "ClosedPNL": float(row['ClosedPNL']) if row['ClosedPNL'] is not None else 0.0
                }
                open_orders_rows.append(clean_row)
            
            table_id = f"{project_id}.{dataset_id}.OpenOrders"
            errors = bq_client.insert_rows_json(table_id, open_orders_rows)
            if errors:
                console.print(f"Errors occurred while inserting open orders data: {errors}", style="bold red")
        
        # Insert Summary data only if positions_df has data
        if not positions_df.empty and 'OpenPNL' in positions_df.columns:
            # Calculate sums, replacing NaN with 0
            # Using convert_dtypes() to properly handle type conversions
            total_open_pnl = positions_df['OpenPNL'].fillna(0).astype(float).sum()
            total_closed_pnl = positions_df['ClosedPNL'].fillna(0).astype(float).sum()
            total_total = positions_df['Total'].fillna(0).astype(float).sum()
            
            # Use explicit type conversion to avoid warnings
            total_weighted = positions_df['TotalWeighted'].fillna(0).astype(float).sum() if 'TotalWeighted' in positions_df else 0.0
            total_equal_weighted = positions_df['EqualWeighted'].fillna(0).astype(float).sum() if 'EqualWeighted' in positions_df else 0.0
            total_mm_weighted = positions_df['MMWeighted'].fillna(0).astype(float).sum() if 'MMWeighted' in positions_df else 0.0
            total_btc_weighted = positions_df['BTCWeighted'].fillna(0).astype(float).sum() if 'BTCWeighted' in positions_df else 0.0
            
            summary_row = {
                "Timestamp": timestamp,
                "TotalOpenPNL": float(total_open_pnl),
                "TotalClosedPNL": float(total_closed_pnl),
                "TotalTotal": float(total_total),
                "TotalWeighted": float(total_weighted),
                "TotalEqualWeighted": float(total_equal_weighted),
                "TotalMMWeighted": float(total_mm_weighted),
                "BTCWeighted": float(total_btc_weighted),
                "WeightedSharePrice": float(share_prices.get('weighted', 0)),
                "EqualSharePrice": float(share_prices.get('equal', 0)),
                "MMSharePrice": float(share_prices.get('MM', 0)),
                "BTCSharePrice": float(share_prices.get('btc', 0))
            }
            
            table_id = f"{project_id}.{dataset_id}.Summary"
            errors = bq_client.insert_rows_json(table_id, [summary_row])
            if errors:
                console.print(f"Errors occurred while inserting summary data: {errors}", style="bold red")
        
        console.print(f"Data successfully uploaded to BigQuery at {timestamp}", style="bold green")
    except Exception as e:
        console.print(f"BigQuery upload error: {e}", style="bold red")
        logging.error(f"BigQuery upload error: {e}\n{traceback.format_exc()}")
        raise e

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
    
    # Initialize BigQuery client when starting
    try:
        bq_client = initialize_bigquery_client()
        console.print("BigQuery client initialized successfully", style="bold green")
    except Exception as e:
        console.print(f"Error initializing BigQuery client: {e}", style="bold red")
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

            save_data_to_bigquery(bq_client, positions_df, open_orders_df, share_prices)

        except Exception as e:
            error_msg = f"An error occurred: {str(e)}\n{traceback.format_exc()}"
            logging.error(error_msg)
            console.print(f"An error occurred. Check error_log.txt for details.", style="bold red")

        finally:
            # Wait for 60 seconds before refreshing
            time.sleep(10)

if __name__ == "__main__":
    main()
