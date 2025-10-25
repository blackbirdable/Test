#!/usr/bin/env python
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import datetime, timedelta

# BigQuery configuration
PROJECT_ID = 'massive-clone-400221'
KEY_PATH = r'C:\Users\Administrator\Desktop\Sonixen\Tools\Ctrader OpenAPI\massive-clone-400221-a39952ec190a.json'

# Dataset configurations
PORTFOLIO_DATASET = 'benchmark_portfolio'
MARKET_DATA_DATASET = 'financial_data'
MARKET_DATA_TABLE = 'market_quotes'

# Get script directory for default output
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Initialize BigQuery client
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

def fetch_portfolio_tables():
    """Fetch the list of available tables from the portfolio dataset"""
    try:
        tables = list(bq_client.list_tables(f"{PROJECT_ID}.{PORTFOLIO_DATASET}"))
        return [table.table_id for table in tables]
    except Exception as e:
        print(f"Error fetching portfolio tables: {e}")
        return ["Positions", "OpenOrders", "Summary"]  # Return default tables if error

def fetch_available_symbols():
    """Fetch the list of available symbols from market data table"""
    try:
        query = f"""
        SELECT DISTINCT symbol
        FROM `{PROJECT_ID}.{MARKET_DATA_DATASET}.{MARKET_DATA_TABLE}`
        ORDER BY symbol
        """
        
        query_job = bq_client.query(query)
        rows = query_job.result()
        
        # Convert to list
        symbols = [row.symbol for row in rows]
        return symbols
    except Exception as e:
        print(f"Error fetching symbols: {e}")
        return []

def get_table_columns(dataset_id, table_id):
    """Get the column names for a specified table"""
    try:
        table_ref = bq_client.dataset(dataset_id).table(table_id)
        table = bq_client.get_table(table_ref)
        return [field.name for field in table.schema]
    except Exception as e:
        print(f"Error fetching columns for {table_id}: {e}")
        return []

def fetch_portfolio_data(table_name, columns=None, start_date=None, end_date=None, limit=1000):
    """
    Fetch data from portfolio table with filtering options
    
    Args:
        table_name (str): Table name to query
        columns (list): List of columns to select (None means all columns)
        start_date (str): Start date in format 'YYYY-MM-DD'
        end_date (str): End date in format 'YYYY-MM-DD'
        limit (int): Maximum number of rows to return
    """
    # Build column selection
    column_str = "*" if not columns or len(columns) == 0 else ", ".join(columns)
    
    query = f"""
    SELECT {column_str}
    FROM `{PROJECT_ID}.{PORTFOLIO_DATASET}.{table_name}`
    WHERE 1=1
    """
    
    # Add date range filters if provided
    if start_date:
        query += f"\nAND Timestamp >= '{start_date}'"
    if end_date:
        query += f"\nAND Timestamp <= '{end_date} 23:59:59'"
    
    query += f"""
    ORDER BY Timestamp DESC
    LIMIT {limit}
    """
    
    print(f"Executing query: {query}")
    query_job = bq_client.query(query)
    
    # Wait for the query to complete
    rows = query_job.result()
    
    # Convert to pandas DataFrame
    df = rows.to_dataframe()
    
    if df.empty:
        print("No data found with the specified filters.")
        return None
    
    print(f"Retrieved {len(df)} rows of data")
    return df

def fetch_market_data(symbols=None, columns=None, start_date=None, end_date=None, limit=1000):
    """
    Fetch quotes from market data table with filter options
    
    Args:
        symbols (list): List of symbols to filter by
        columns (list): List of columns to select (None means all columns)
        start_date (str): Start date in format 'YYYY-MM-DD'
        end_date (str): End date in format 'YYYY-MM-DD'
        limit (int): Maximum number of rows to return
    """
    # Build column selection
    column_str = "*" if not columns or len(columns) == 0 else ", ".join(columns)
    
    query = f"""
    SELECT {column_str}
    FROM `{PROJECT_ID}.{MARKET_DATA_DATASET}.{MARKET_DATA_TABLE}`
    WHERE 1=1
    """
    
    # Add symbol filter if provided
    if symbols and len(symbols) > 0:
        symbols_str = "', '".join(symbols)
        query += f"\nAND symbol IN ('{symbols_str}')"
    
    # Add date range filters if provided
    if start_date:
        query += f"\nAND timestamp >= '{start_date}'"
    if end_date:
        query += f"\nAND timestamp <= '{end_date}'"
    
    query += f"""
    ORDER BY timestamp DESC
    LIMIT {limit}
    """
    
    print(f"Executing query: {query}")
    query_job = bq_client.query(query)
    
    # Wait for the query to complete
    rows = query_job.result()
    
    # Convert to pandas DataFrame
    df = rows.to_dataframe()
    
    if df.empty:
        print("No data found with the specified filters.")
        return None
    
    print(f"Retrieved {len(df)} rows of data")
    return df

def save_to_csv(df, output_path):
    """Save DataFrame to CSV file"""
    df.to_csv(output_path, index=False)
    print(f"Data saved to {output_path}")

class ConsolidatedDataDownloaderApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Financial Data Downloader")
        self.root.geometry("800x700")
        self.root.resizable(True, True)
        
        # Configure the main frame with a notebook for tabs
        main_frame = ttk.Frame(root, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Title
        title_label = ttk.Label(main_frame, text="Consolidated Financial Data Downloader", font=("Arial", 16, "bold"))
        title_label.pack(anchor="w", pady=(0, 20))
        
        # Create notebook for tabs
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        # Create tabs
        self.portfolio_tab = ttk.Frame(self.notebook)
        self.market_data_tab = ttk.Frame(self.notebook)
        
        self.notebook.add(self.portfolio_tab, text="Portfolio Data")
        self.notebook.add(self.market_data_tab, text="Market Data")
        
        # Initialize tab contents
        self.init_portfolio_tab()
        self.init_market_data_tab()
        
        # Status frame at the bottom across both tabs
        status_frame = ttk.LabelFrame(main_frame, text="Status", padding=(10, 5))
        status_frame.pack(fill=tk.BOTH, expand=False, pady=(20, 0))
        
        # Status text
        self.status_text = tk.Text(status_frame, height=8, width=80, wrap=tk.WORD)
        self.status_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Scrollbar for status text
        status_scrollbar = ttk.Scrollbar(status_frame, orient="vertical", command=self.status_text.yview)
        status_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.status_text.config(yscrollcommand=status_scrollbar.set)
        
        # Redirect print to the status text widget
        self.original_stdout = None
        self.redirect_stdout()
    
    def redirect_stdout(self):
        """Redirect standard output to the status text widget"""
        class StdoutRedirector:
            def __init__(self, text_widget):
                self.text_widget = text_widget
                
            def write(self, message):
                self.text_widget.insert(tk.END, message)
                self.text_widget.see(tk.END)
                
            def flush(self):
                pass
        
        self.original_stdout = os.sys.stdout
        os.sys.stdout = StdoutRedirector(self.status_text)
    
    def init_portfolio_tab(self):
        """Initialize the portfolio tab content"""
        # Configure the portfolio tab
        frame = ttk.Frame(self.portfolio_tab, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)
        
        # Table selection
        ttk.Label(frame, text="Table:", font=("Arial", 10, "bold")).grid(row=0, column=0, sticky="w")
        
        # Load available tables
        self.portfolio_tables = fetch_portfolio_tables()
        
        # Table dropdown
        self.portfolio_table_var = tk.StringVar(value=self.portfolio_tables[0] if self.portfolio_tables else "")
        table_dropdown = ttk.Combobox(frame, textvariable=self.portfolio_table_var, values=self.portfolio_tables, state="readonly", width=30)
        table_dropdown.grid(row=0, column=1, columnspan=2, sticky="w")
        table_dropdown.bind("<<ComboboxSelected>>", self.on_portfolio_table_selected)
        
        # Column selection frame
        self.portfolio_columns_frame = ttk.LabelFrame(frame, text="Select Columns", padding=(10, 5))
        self.portfolio_columns_frame.grid(row=1, column=0, columnspan=3, sticky="ew", pady=(10, 0))
        
        # Will be populated when a table is selected
        self.portfolio_column_vars = {}
        
        # Date range selection
        date_frame = ttk.LabelFrame(frame, text="Date Range", padding=(10, 5))
        date_frame.grid(row=2, column=0, columnspan=3, sticky="ew", pady=(10, 0))
        
        # Start date
        ttk.Label(date_frame, text="Start Date:").grid(row=0, column=0, sticky="w")
        self.portfolio_start_date_var = tk.StringVar(value=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
        self.portfolio_start_date_entry = ttk.Entry(date_frame, textvariable=self.portfolio_start_date_var, width=12)
        self.portfolio_start_date_entry.grid(row=0, column=1, padx=(5, 10), pady=5)
        
        # End date - Changed to entry field
        ttk.Label(date_frame, text="End Date:").grid(row=0, column=2, sticky="w")
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        self.portfolio_end_date_var = tk.StringVar(value=tomorrow)
        self.portfolio_end_date_entry = ttk.Entry(date_frame, textvariable=self.portfolio_end_date_var, width=12)
        self.portfolio_end_date_entry.grid(row=0, column=3, padx=(5, 10), pady=5)
        
        # Preset date ranges
        ttk.Label(date_frame, text="Preset:").grid(row=1, column=0, sticky="w")
        preset_frame = ttk.Frame(date_frame)
        preset_frame.grid(row=1, column=1, columnspan=3, sticky="w")
        
        preset_options = [
            ("Last 7 Days", 7),
            ("Last 30 Days", 30),
            ("Last 90 Days", 90),
            ("Last 365 Days", 365),
            ("Year to Date", -1)  # Special case
        ]
        
        col = 0
        for text, days in preset_options:
            btn = ttk.Button(preset_frame, text=text, width=12, 
                         command=lambda d=days: self.set_portfolio_preset_date_range(d))
            btn.grid(row=0, column=col, padx=(0, 5))
            col += 1
        
        # Output options
        output_frame = ttk.LabelFrame(frame, text="Output Options", padding=(10, 5))
        output_frame.grid(row=3, column=0, columnspan=3, sticky="ew", pady=(10, 0))
        
        # Output directory - Changed to use script directory
        ttk.Label(output_frame, text="Output Directory:").grid(row=0, column=0, sticky="w")
        self.portfolio_output_dir_var = tk.StringVar(value=SCRIPT_DIR)
        self.portfolio_output_dir_entry = ttk.Entry(output_frame, textvariable=self.portfolio_output_dir_var, width=50)
        self.portfolio_output_dir_entry.grid(row=0, column=1, padx=(5, 5), pady=5, sticky="ew")
        
        # Browse button
        ttk.Button(output_frame, text="Browse...", command=self.browse_portfolio_output_dir).grid(row=0, column=2, padx=(0, 0), pady=5)
        
        # Row limit
        ttk.Label(output_frame, text="Row Limit:").grid(row=1, column=0, sticky="w")
        self.portfolio_limit_var = tk.StringVar(value="1000")
        limit_entry = ttk.Entry(output_frame, textvariable=self.portfolio_limit_var, width=10)
        limit_entry.grid(row=1, column=1, sticky="w", padx=(5, 0), pady=5)
        
        # File name prefix
        ttk.Label(output_frame, text="File Prefix:").grid(row=2, column=0, sticky="w")
        self.portfolio_file_prefix_var = tk.StringVar(value="portfolio_data")
        file_prefix_entry = ttk.Entry(output_frame, textvariable=self.portfolio_file_prefix_var, width=20)
        file_prefix_entry.grid(row=2, column=1, sticky="w", padx=(5, 0), pady=5)
        
        # Buttons frame
        buttons_frame = ttk.Frame(frame)
        buttons_frame.grid(row=4, column=0, columnspan=3, pady=(20, 0), sticky="ew")
        
        # Preview button
        preview_button = ttk.Button(buttons_frame, text="Preview Data", command=self.preview_portfolio_data, width=15)
        preview_button.pack(side=tk.LEFT, padx=(0, 5))
        
        # Row count indicator
        self.portfolio_row_count_var = tk.StringVar(value="N/A")
        ttk.Label(buttons_frame, text="Estimated row count:").pack(side=tk.LEFT, padx=(10, 5))
        ttk.Label(buttons_frame, textvariable=self.portfolio_row_count_var).pack(side=tk.LEFT)
        
        # Fetch and save button
        fetch_button = ttk.Button(buttons_frame, text="Download Data", command=self.fetch_and_save_portfolio, width=15)
        fetch_button.pack(side=tk.RIGHT, padx=(5, 0))
        
        # Initialize column selection if we have tables
        if self.portfolio_tables:
            self.on_portfolio_table_selected(None)
    
    def init_market_data_tab(self):
        """Initialize the market data tab content"""
        # Configure the market data tab
        frame = ttk.Frame(self.market_data_tab, padding="10")
        frame.pack(fill=tk.BOTH, expand=True)
        
        # Symbol selection
        ttk.Label(frame, text="Symbols:", font=("Arial", 10, "bold")).grid(row=0, column=0, sticky="w")
        
        # Frame for symbol selection
        symbols_frame = ttk.Frame(frame)
        symbols_frame.grid(row=1, column=0, columnspan=3, sticky="ew")
        
        # Load available symbols
        self.available_symbols = fetch_available_symbols()
        
        # Option to select all symbols
        self.all_symbols_var = tk.BooleanVar()
        ttk.Checkbutton(symbols_frame, text="All Symbols", variable=self.all_symbols_var, 
                     command=self.toggle_symbol_selection).grid(row=0, column=0, sticky="w")
        
        # Symbol entry with autocomplete for manual input
        ttk.Label(symbols_frame, text="Or enter symbols (comma separated):").grid(row=1, column=0, sticky="w", pady=(10, 0))
        self.symbols_entry = ttk.Entry(symbols_frame, width=50)
        self.symbols_entry.grid(row=2, column=0, sticky="ew", pady=(5, 0))
        
        # Symbol list box (if we have many available)
        if len(self.available_symbols) > 0:
            ttk.Label(symbols_frame, text="Or select from available:").grid(row=3, column=0, sticky="w", pady=(10, 0))
            self.symbols_listbox_frame = ttk.Frame(symbols_frame)
            self.symbols_listbox_frame.grid(row=4, column=0, sticky="ew", pady=(5, 0))
            
            self.symbols_listbox = tk.Listbox(self.symbols_listbox_frame, selectmode=tk.MULTIPLE, height=5)
            self.symbols_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            
            scrollbar = ttk.Scrollbar(self.symbols_listbox_frame, orient="vertical", command=self.symbols_listbox.yview)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            self.symbols_listbox.config(yscrollcommand=scrollbar.set)
            
            # Populate the listbox
            for symbol in self.available_symbols:
                self.symbols_listbox.insert(tk.END, symbol)
        
        # Column selection frame - load on tab initialization for market data
        self.market_data_column_vars = {}
        self.market_columns_frame = ttk.LabelFrame(frame, text="Select Columns", padding=(10, 5))
        self.market_columns_frame.grid(row=2, column=0, columnspan=3, sticky="ew", pady=(10, 0))
        
        # Populate market data columns
        self.populate_market_data_columns()
        
        # Date range selection
        date_frame = ttk.LabelFrame(frame, text="Date Range", padding=(10, 5))
        date_frame.grid(row=3, column=0, columnspan=3, sticky="ew", pady=(10, 0))
        
        # Start date
        ttk.Label(date_frame, text="Start Date:").grid(row=0, column=0, sticky="w")
        self.market_start_date_var = tk.StringVar(value=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
        self.market_start_date_entry = ttk.Entry(date_frame, textvariable=self.market_start_date_var, width=12)
        self.market_start_date_entry.grid(row=0, column=1, padx=(5, 10), pady=5)
        
        # End date - Changed to entry field
        ttk.Label(date_frame, text="End Date:").grid(row=0, column=2, sticky="w")
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        self.market_end_date_var = tk.StringVar(value=tomorrow)
        self.market_end_date_entry = ttk.Entry(date_frame, textvariable=self.market_end_date_var, width=12)
        self.market_end_date_entry.grid(row=0, column=3, padx=(5, 10), pady=5)
        
        # Preset date ranges
        ttk.Label(date_frame, text="Preset:").grid(row=1, column=0, sticky="w")
        preset_frame = ttk.Frame(date_frame)
        preset_frame.grid(row=1, column=1, columnspan=3, sticky="w")
        
        preset_options = [
            ("Last 7 Days", 7),
            ("Last 30 Days", 30),
            ("Last 90 Days", 90),
            ("Last 365 Days", 365),
            ("Year to Date", -1)  # Special case
        ]
        
        col = 0
        for text, days in preset_options:
            btn = ttk.Button(preset_frame, text=text, width=12, 
                         command=lambda d=days: self.set_market_preset_date_range(d))
            btn.grid(row=0, column=col, padx=(0, 5))
            col += 1
        
        # Output options
        output_frame = ttk.LabelFrame(frame, text="Output Options", padding=(10, 5))
        output_frame.grid(row=4, column=0, columnspan=3, sticky="ew", pady=(10, 0))
        
        # Output directory - Changed to use script directory
        ttk.Label(output_frame, text="Output Directory:").grid(row=0, column=0, sticky="w")
        self.market_output_dir_var = tk.StringVar(value=SCRIPT_DIR)
        self.market_output_dir_entry = ttk.Entry(output_frame, textvariable=self.market_output_dir_var, width=50)
        self.market_output_dir_entry.grid(row=0, column=1, padx=(5, 5), pady=5, sticky="ew")
        
        # Browse button
        ttk.Button(output_frame, text="Browse...", command=self.browse_market_output_dir).grid(row=0, column=2, padx=(0, 0), pady=5)
        
        # Row limit
        ttk.Label(output_frame, text="Row Limit:").grid(row=1, column=0, sticky="w")
        self.market_limit_var = tk.StringVar(value="1000")
        limit_entry = ttk.Entry(output_frame, textvariable=self.market_limit_var, width=10)
        limit_entry.grid(row=1, column=1, sticky="w", padx=(5, 0), pady=5)
        
        # File name prefix
        ttk.Label(output_frame, text="File Prefix:").grid(row=2, column=0, sticky="w")
        self.market_file_prefix_var = tk.StringVar(value="market_quotes")
        file_prefix_entry = ttk.Entry(output_frame, textvariable=self.market_file_prefix_var, width=20)
        file_prefix_entry.grid(row=2, column=1, sticky="w", padx=(5, 0), pady=5)
        
        # Buttons frame
        buttons_frame = ttk.Frame(frame)
        buttons_frame.grid(row=5, column=0, columnspan=3, pady=(20, 0), sticky="ew")
        
        # Preview button
        preview_button = ttk.Button(buttons_frame, text="Preview Data", command=self.preview_market_data, width=15)
        preview_button.pack(side=tk.LEFT, padx=(0, 5))
        
        # Row count indicator
        self.market_row_count_var = tk.StringVar(value="N/A")
        ttk.Label(buttons_frame, text="Estimated row count:").pack(side=tk.LEFT, padx=(10, 5))
        ttk.Label(buttons_frame, textvariable=self.market_row_count_var).pack(side=tk.LEFT)
        
        # Fetch and save button
        fetch_button = ttk.Button(buttons_frame, text="Download Data", command=self.fetch_and_save_market, width=15)
        fetch_button.pack(side=tk.RIGHT, padx=(5, 0))
    
    def on_portfolio_table_selected(self, event):
        """Handle portfolio table selection event"""
        # Clear column selection frame
        for widget in self.portfolio_columns_frame.winfo_children():
            widget.destroy()
        
        # Reset column variables
        self.portfolio_column_vars = {}
        
        # Get columns for selected table
        table_name = self.portfolio_table_var.get()
        if not table_name:
            return
        
        columns = get_table_columns(PORTFOLIO_DATASET, table_name)
        
        # Create column checkboxes
        # Use a canvas with scrollbar if there are many columns
        if len(columns) > 10:
            canvas = tk.Canvas(self.portfolio_columns_frame)
            scrollbar = ttk.Scrollbar(self.portfolio_columns_frame, orient="vertical", command=canvas.yview)
            scroll_frame = ttk.Frame(canvas)
            
            # Configure scrolling
            scroll_frame.bind(
                "<Configure>",
                lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
            )
            
            canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
            canvas.configure(yscrollcommand=scrollbar.set)
            
            canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            
            container = scroll_frame
        else:
            container = self.portfolio_columns_frame
        
        # Select all checkbox
        self.portfolio_select_all_var = tk.BooleanVar(value=True)
        select_all_cb = ttk.Checkbutton(container, text="Select All", 
                                     variable=self.portfolio_select_all_var,
                                     command=self.toggle_portfolio_columns)
        select_all_cb.grid(row=0, column=0, sticky="w", pady=(0, 5))
        
        # Add checkboxes for each column
        row, col = 1, 0
        for column in columns:
            var = tk.BooleanVar(value=True)
            self.portfolio_column_vars[column] = var
            
            cb = ttk.Checkbutton(container, text=column, variable=var)
            cb.grid(row=row, column=col, sticky="w", padx=(5, 10))
            
            col += 1
            if col > 2:  # 3 columns of checkboxes
                col = 0
                row += 1
    
    def populate_market_data_columns(self):
        """Populate market data column selection"""
        # Clear column selection frame
        for widget in self.market_columns_frame.winfo_children():
            widget.destroy()
        
        # Reset column variables
        self.market_data_column_vars = {}
        
        # Get columns for market data table
        columns = get_table_columns(MARKET_DATA_DATASET, MARKET_DATA_TABLE)
        
        # Create column checkboxes
        # Use a canvas with scrollbar if there are many columns
        if len(columns) > 10:
            canvas = tk.Canvas(self.market_columns_frame)
            scrollbar = ttk.Scrollbar(self.market_columns_frame, orient="vertical", command=canvas.yview)
            scroll_frame = ttk.Frame(canvas)
            
            # Configure scrolling
            scroll_frame.bind(
                "<Configure>",
                lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
            )
            
            canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
            canvas.configure(yscrollcommand=scrollbar.set)
            
            canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            
            container = scroll_frame
        else:
            container = self.market_columns_frame
        
        # Select all checkbox
        self.market_select_all_var = tk.BooleanVar(value=True)
        select_all_cb = ttk.Checkbutton(container, text="Select All", 
                                     variable=self.market_select_all_var,
                                     command=self.toggle_market_columns)
        select_all_cb.grid(row=0, column=0, sticky="w", pady=(0, 5))
        
        # Add checkboxes for each column
        row, col = 1, 0
        for column in columns:
            var = tk.BooleanVar(value=True)
            self.market_data_column_vars[column] = var
            
            cb = ttk.Checkbutton(container, text=column, variable=var)
            cb.grid(row=row, column=col, sticky="w", padx=(5, 10))
            
            col += 1
            if col > 2:  # 3 columns of checkboxes
                col = 0
                row += 1
    
    def toggle_portfolio_columns(self):
        """Toggle all portfolio columns based on select all checkbox"""
        select_all = self.portfolio_select_all_var.get()
        for var in self.portfolio_column_vars.values():
            var.set(select_all)
    
    def toggle_market_columns(self):
        """Toggle all market data columns based on select all checkbox"""
        select_all = self.market_select_all_var.get()
        for var in self.market_data_column_vars.values():
            var.set(select_all)
    
    def toggle_symbol_selection(self):
        """Handle the 'All Symbols' checkbox"""
        if self.all_symbols_var.get():
            self.symbols_entry.config(state="disabled")
            if hasattr(self, 'symbols_listbox'):
                self.symbols_listbox.config(state="disabled")
        else:
            self.symbols_entry.config(state="normal")
            if hasattr(self, 'symbols_listbox'):
                self.symbols_listbox.config(state="normal")
    
    def set_portfolio_preset_date_range(self, days):
        """Set a preset date range for portfolio data"""
        end_date = datetime.now() + timedelta(days=1)  # Tomorrow
        
        if days == -1:  # Year to Date
            start_date = datetime(datetime.now().year, 1, 1)
        else:
            start_date = datetime.now() - timedelta(days=days)
        
        self.portfolio_start_date_var.set(start_date.strftime("%Y-%m-%d"))
        self.portfolio_end_date_var.set(end_date.strftime("%Y-%m-%d"))
    
    def set_market_preset_date_range(self, days):
        """Set a preset date range for market data"""
        end_date = datetime.now() + timedelta(days=1)  # Tomorrow
        
        if days == -1:  # Year to Date
            start_date = datetime(datetime.now().year, 1, 1)
        else:
            start_date = datetime.now() - timedelta(days=days)
        
        self.market_start_date_var.set(start_date.strftime("%Y-%m-%d"))
        self.market_end_date_var.set(end_date.strftime("%Y-%m-%d"))
    
    def browse_portfolio_output_dir(self):
        """Browse for portfolio output directory"""
        directory = filedialog.askdirectory(initialdir=self.portfolio_output_dir_var.get())
        if directory:
            self.portfolio_output_dir_var.set(directory)
    
    def browse_market_output_dir(self):
        """Browse for market data output directory"""
        directory = filedialog.askdirectory(initialdir=self.market_output_dir_var.get())
        if directory:
            self.market_output_dir_var.set(directory)
    
    def get_selected_portfolio_columns(self):
        """Get the list of selected portfolio columns"""
        selected = []
        for column, var in self.portfolio_column_vars.items():
            if var.get():
                selected.append(column)
        return selected if selected else None
    
    def get_selected_market_columns(self):
        """Get the list of selected market data columns"""
        selected = []
        for column, var in self.market_data_column_vars.items():
            if var.get():
                selected.append(column)
        return selected if selected else None
    
    def get_selected_symbols(self):
        """Get the list of selected symbols for market data"""
        if self.all_symbols_var.get():
            return None  # All symbols
        
        symbols = []
        
        # Get symbols from entry field
        entry_text = self.symbols_entry.get().strip()
        if entry_text:
            symbols.extend([s.strip() for s in entry_text.split(',') if s.strip()])
        
        # Get symbols from listbox
        if hasattr(self, 'symbols_listbox'):
            selected_indices = self.symbols_listbox.curselection()
            for index in selected_indices:
                symbols.append(self.symbols_listbox.get(index))
        
        return symbols if symbols else None
    
    def preview_portfolio_data(self):
        """Preview portfolio data with current settings"""
        try:
            table_name = self.portfolio_table_var.get()
            if not table_name:
                messagebox.showwarning("Warning", "Please select a table.")
                return
            
            columns = self.get_selected_portfolio_columns()
            start_date = self.portfolio_start_date_var.get() if self.portfolio_start_date_var.get() else None
            end_date = self.portfolio_end_date_var.get() if self.portfolio_end_date_var.get() else None
            
            # Fetch a small sample for preview
            df = fetch_portfolio_data(table_name, columns, start_date, end_date, limit=10)
            
            if df is not None:
                self.portfolio_row_count_var.set(f"{len(df)} (preview)")
                print(f"Preview of {table_name}:")
                print(df.head())
                print(f"Shape: {df.shape}")
            else:
                self.portfolio_row_count_var.set("0")
                
        except Exception as e:
            messagebox.showerror("Error", f"Error previewing data: {str(e)}")
    
    def preview_market_data(self):
        """Preview market data with current settings"""
        try:
            symbols = self.get_selected_symbols()
            columns = self.get_selected_market_columns()
            start_date = self.market_start_date_var.get() if self.market_start_date_var.get() else None
            end_date = self.market_end_date_var.get() if self.market_end_date_var.get() else None
            
            # Fetch a small sample for preview
            df = fetch_market_data(symbols, columns, start_date, end_date, limit=10)
            
            if df is not None:
                self.market_row_count_var.set(f"{len(df)} (preview)")
                print(f"Preview of market data:")
                print(df.head())
                print(f"Shape: {df.shape}")
            else:
                self.market_row_count_var.set("0")
                
        except Exception as e:
            messagebox.showerror("Error", f"Error previewing data: {str(e)}")
    
    def fetch_and_save_portfolio(self):
        """Fetch and save portfolio data"""
        try:
            table_name = self.portfolio_table_var.get()
            if not table_name:
                messagebox.showwarning("Warning", "Please select a table.")
                return
            
            columns = self.get_selected_portfolio_columns()
            start_date = self.portfolio_start_date_var.get() if self.portfolio_start_date_var.get() else None
            end_date = self.portfolio_end_date_var.get() if self.portfolio_end_date_var.get() else None
            limit = int(self.portfolio_limit_var.get()) if self.portfolio_limit_var.get() else 1000
            
            # Fetch data
            df = fetch_portfolio_data(table_name, columns, start_date, end_date, limit)
            
            if df is not None:
                # Generate filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{self.portfolio_file_prefix_var.get()}_{table_name}_{timestamp}.csv"
                output_path = os.path.join(self.portfolio_output_dir_var.get(), filename)
                
                # Save to CSV
                save_to_csv(df, output_path)
                
                messagebox.showinfo("Success", f"Data saved successfully to:\n{output_path}")
            else:
                messagebox.showwarning("Warning", "No data found with the specified filters.")
                
        except Exception as e:
            messagebox.showerror("Error", f"Error fetching and saving data: {str(e)}")
    
    def fetch_and_save_market(self):
        """Fetch and save market data"""
        try:
            symbols = self.get_selected_symbols()
            columns = self.get_selected_market_columns()
            start_date = self.market_start_date_var.get() if self.market_start_date_var.get() else None
            end_date = self.market_end_date_var.get() if self.market_end_date_var.get() else None
            limit = int(self.market_limit_var.get()) if self.market_limit_var.get() else 1000
            
            # Fetch data
            df = fetch_market_data(symbols, columns, start_date, end_date, limit)
            
            if df is not None:
                # Generate filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                symbols_str = "all" if symbols is None else "_".join(symbols[:3])  # Use first 3 symbols
                filename = f"{self.market_file_prefix_var.get()}_{symbols_str}_{timestamp}.csv"
                output_path = os.path.join(self.market_output_dir_var.get(), filename)
                
                # Save to CSV
                save_to_csv(df, output_path)
                
                messagebox.showinfo("Success", f"Data saved successfully to:\n{output_path}")
            else:
                messagebox.showwarning("Warning", "No data found with the specified filters.")
                
        except Exception as e:
            messagebox.showerror("Error", f"Error fetching and saving data: {str(e)}")

if __name__ == "__main__":
    root = tk.Tk()
    app = ConsolidatedDataDownloaderApp(root)
    root.mainloop()
