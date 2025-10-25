import pandas as pd
import sqlite3
import os
import time
import datetime
import logging
import traceback
from google.cloud import bigquery
from google.oauth2 import service_account
import platform
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('emsbtd_sync.log'),
        logging.StreamHandler()
    ]
)

# Global variables
TAKE_PROFIT_PERCENTAGE = 1.5  # 1.5% take profit from entry price of WeightedSharePrice
STOP_LOSS_PERCENTAGE = 1.0    # 1.0% stop loss from entry price of WeightedSharePrice
DIP_THRESHOLD_PERCENTAGE = 2.0  # 2% dip to trigger mirroring
CANDLE_PERIOD_MINUTES = 25    # OHLC candle period in minutes
MAX_ROWS_TO_KEEP = 1000       # Maximum number of rows to keep in the database

# Configuration
class Config:
    def __init__(self):
        self.is_windows = platform.system() == 'Windows'
        
        # BigQuery configuration
        if self.is_windows:
            self.credentials_path = r"C:\Users\Administrator\Desktop\Sonixen\Tools\Ctrader OpenAPI\massive-clone-400221-a39952ec190a.json"
            self.db_path = r"C:\Users\Administrator\Desktop\TradingData_BTD_AC2.db"
        else:
            self.credentials_path = r"/home/bryan_oosterveld/credentials/bigquery_credentials_placeholder.json"
            self.db_path = r"/home/bryan_oosterveld/TradingData_BTD_AC2.db"
            
        self.project_id = "massive-clone-400221"
        self.dataset_id = "benchmark_portfolio"
        self.sync_interval = 60  # Sync from BigQuery every 60 seconds


class BigQueryClient:
    def __init__(self, config):
        self.config = config
        self.client = None
        self.initialize_client()
        
    def initialize_client(self):
        """Initialize and return a BigQuery client."""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.config.credentials_path, 
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            self.client = bigquery.Client(credentials=credentials, project=self.config.project_id)
            logging.info(f"BigQuery client initialized successfully")
            return True
        except Exception as e:
            logging.error(f"Error initializing BigQuery client: {e}\n{traceback.format_exc()}")
            return False
    
    def fetch_positions(self, limit=1):
        """Fetch most recent position data from BigQuery."""
        query = f"""
        SELECT * FROM `{self.config.project_id}.{self.config.dataset_id}.Positions`
        ORDER BY Timestamp DESC
        LIMIT {limit}
        """
        try:
            return self.client.query(query).to_dataframe()
        except Exception as e:
            logging.error(f"Error fetching positions: {e}\n{traceback.format_exc()}")
            return pd.DataFrame()
    
    def fetch_open_orders(self, limit=1):
        """Fetch most recent open orders data from BigQuery."""
        query = f"""
        SELECT * FROM `{self.config.project_id}.{self.config.dataset_id}.OpenOrders`
        ORDER BY Timestamp DESC
        LIMIT {limit}
        """
        try:
            return self.client.query(query).to_dataframe()
        except Exception as e:
            logging.error(f"Error fetching open orders: {e}\n{traceback.format_exc()}")
            return pd.DataFrame()
    
    def fetch_summary(self, limit=1):
        """Fetch most recent summary data from BigQuery."""
        query = f"""
        SELECT * FROM `{self.config.project_id}.{self.config.dataset_id}.Summary`
        ORDER BY Timestamp DESC
        LIMIT {limit}
        """
        try:
            return self.client.query(query).to_dataframe()
        except Exception as e:
            logging.error(f"Error fetching summary: {e}\n{traceback.format_exc()}")
            return pd.DataFrame()


class SQLiteDatabase:
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.initialize_database()
        
    def initialize_database(self):
        """Initialize the SQLite database and create tables if they don't exist."""
        try:
            self.conn = sqlite3.connect(self.config.db_path)
            cursor = self.conn.cursor()
            
            # Create Positions table
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
                CurrentSL TEXT,
                CurrentTP TEXT,
                OpenPNL REAL,
                ClosedPNL REAL,
                Total REAL,
                TotalWeighted REAL,
                EqualWeighted REAL,
                MMWeighted REAL,
                BTCWeighted REAL
            )
            ''')
            
            # Create OpenOrders table
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
                CurrentSL TEXT,
                CurrentTP TEXT,
                OpenPNL REAL,
                ClosedPNL REAL
            )
            ''')
            
            # Create Summary table
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
            
            # Create OHLC table with additional percentage columns and entry/exit price
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS OHLC (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                Timestamp TEXT NOT NULL,
                Open REAL,
                High REAL,
                Low REAL,
                Close REAL,
                HighPct REAL,
                LowPct REAL,
                ClosePct REAL,
                EntryPrice REAL,
                ExitPrice REAL,
                CandlePeriod INTEGER
            )
            ''')
            
            self.conn.commit()
            logging.info(f"SQLite database initialized at {self.config.db_path}")
            return True
        except sqlite3.Error as e:
            logging.error(f"SQLite database initialization error: {e}\n{traceback.format_exc()}")
            return False
    
    def store_positions(self, positions_df):
        """Store positions data in SQLite, overwriting previous data."""
        if positions_df.empty:
            logging.info("No positions data to store")
            return 0
        
        try:
            # Clean up DataFrame for SQLite (handle NaN values)
            positions_df = positions_df.fillna({
                'Type': '', 'Label': '', 'Symbol': '', 'Volume': '', 
                'EntryPrice': '', 'CurrentPrice': '', 'CurrentSL': '', 'CurrentTP': '',
                'OpenPNL': 0.0, 'ClosedPNL': 0.0, 'Total': 0.0, 
                'TotalWeighted': 0.0, 'EqualWeighted': 0.0, 'MMWeighted': 0.0, 'BTCWeighted': 0.0
            })
            
            # Truncate the table and insert fresh data
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM Positions")
            self.conn.commit()
            
            # Insert data into SQLite
            positions_df.to_sql('Positions', self.conn, if_exists='append', index=False)
            
            # Return the number of rows inserted
            rows_inserted = len(positions_df)
            logging.info(f"Stored {rows_inserted} rows in Positions table (replaced old data)")
            return rows_inserted
        except Exception as e:
            logging.error(f"Error storing positions data: {e}\n{traceback.format_exc()}")
            return 0
    
    def store_open_orders(self, open_orders_df):
        """Store open orders data in SQLite, overwriting previous data."""
        if open_orders_df.empty:
            logging.info("No open orders data to store")
            return 0
        
        try:
            # Clean up DataFrame for SQLite
            open_orders_df = open_orders_df.fillna({
                'Type': '', 'Label': '', 'Symbol': '', 'Volume': '', 
                'EntryPrice': '', 'CurrentPrice': '', 'CurrentSL': '', 'CurrentTP': '',
                'OpenPNL': 0.0, 'ClosedPNL': 0.0
            })
            
            # Truncate the table and insert fresh data
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM OpenOrders")
            self.conn.commit()
            
            # Insert data into SQLite
            open_orders_df.to_sql('OpenOrders', self.conn, if_exists='append', index=False)
            
            # Return the number of rows inserted
            rows_inserted = len(open_orders_df)
            logging.info(f"Stored {rows_inserted} rows in OpenOrders table (replaced old data)")
            return rows_inserted
        except Exception as e:
            logging.error(f"Error storing open orders data: {e}\n{traceback.format_exc()}")
            return 0
    
    def store_summary(self, summary_df):
        """Store summary data in SQLite, overwriting previous data."""
        if summary_df.empty:
            logging.info("No summary data to store")
            return 0
        
        try:
            # Clean up DataFrame for SQLite
            summary_df = summary_df.fillna({
                'TotalOpenPNL': 0.0, 'TotalClosedPNL': 0.0, 'TotalTotal': 0.0,
                'TotalWeighted': 0.0, 'TotalEqualWeighted': 0.0, 'TotalMMWeighted': 0.0,
                'BTCWeighted': 0.0, 'WeightedSharePrice': 0.0, 'EqualSharePrice': 0.0,
                'MMSharePrice': 0.0, 'BTCSharePrice': 0.0
            })
            
            # Truncate the table and insert fresh data
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM Summary")
            self.conn.commit()
            
            # Insert data into SQLite
            summary_df.to_sql('Summary', self.conn, if_exists='append', index=False)
            
            # Return the number of rows inserted
            rows_inserted = len(summary_df)
            logging.info(f"Stored {rows_inserted} rows in Summary table (replaced old data)")
            return rows_inserted
        except Exception as e:
            logging.error(f"Error storing summary data: {e}\n{traceback.format_exc()}")
            return 0
    
    def get_latest_timestamp(self, table_name):
        """Get the latest timestamp from a specific table."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT Timestamp FROM {table_name} ORDER BY id DESC LIMIT 1")
            result = cursor.fetchone()
            if result:
                return result[0]
            return None
        except sqlite3.Error as e:
            logging.error(f"Error getting latest timestamp from {table_name}: {e}")
            return None
    
    def store_ohlc_candle(self, candle):
        """
        Legacy method - OHLC candles are now stored directly in the FixedOHLCCalculator.store_new_candle_in_db method.
        This method is kept for compatibility but is no longer used.
        """
        logging.warning("store_ohlc_candle method called but is deprecated - OHLC candles are now stored directly in the FixedOHLCCalculator")
        if not candle or 'timestamp' not in candle or candle['open'] is None:
            logging.info("No valid OHLC candle to store")
            return False
        
        try:
            cursor = self.conn.cursor()
            
            # Format timestamp properly for storage
            timestamp_str = candle['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            
            # Insert candle data into the OHLC table
            cursor.execute('''
                INSERT INTO OHLC (Timestamp, Open, High, Low, Close, CandlePeriod)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                timestamp_str,
                candle['open'],
                candle['high'],
                candle['low'],
                candle['close'],
                CANDLE_PERIOD_MINUTES
            ))
            
            self.conn.commit()
            logging.info(f"Stored OHLC candle for {timestamp_str} in database")
            return True
        except sqlite3.Error as e:
            logging.error(f"Error storing OHLC candle: {e}\n{traceback.format_exc()}")
            return False
    
    def get_recent_ohlc_candles(self, limit=30):
        """Retrieve the most recent OHLC candles from the database."""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                SELECT Timestamp, Open, High, Low, Close, HighPct, LowPct, ClosePct, EntryPrice, ExitPrice, CandlePeriod
                FROM OHLC
                ORDER BY datetime(Timestamp) DESC, id DESC
                LIMIT ?
            ''', (limit,))
            
            results = cursor.fetchall()
            candles = []
            
            for row in results:
                try:
                    # Use our more robust parsing method instead of direct strptime
                    timestamp = self.parse_timestamp(row[0])
                    if timestamp is None:
                        logging.warning(f"Failed to parse timestamp: {row[0]}")
                        continue
                except Exception as e:
                    logging.error(f"Error parsing timestamp '{row[0]}': {e}")
                    continue
                
                candle = {
                    'timestamp': timestamp,
                    'open': row[1],
                    'high': row[2],
                    'low': row[3],
                    'close': row[4],
                    'high_pct': row[5],
                    'low_pct': row[6],
                    'close_pct': row[7],
                    'entry_price': row[8],
                    'exit_price': row[9],
                    'period': row[10]
                }
                candles.append(candle)
            
            if candles:
                # Log the most recent candle for debugging
                most_recent = candles[0]  # First one because we sorted DESC
                logging.info(f"Most recent candle from get_recent_ohlc_candles: {most_recent['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Return candles in reverse chronological order (newest first)
            # This change matches with how we're handling candles in load_candles_from_db method
            return candles
        except sqlite3.Error as e:
            logging.error(f"Error retrieving OHLC candles: {e}\n{traceback.format_exc()}")
            return []
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            logging.info("SQLite database connection closed")


class FixedOHLCCalculator:
    def __init__(self, db_path):
        self.interval_minutes = CANDLE_PERIOD_MINUTES  # Use global variable
        self.db_path = db_path
        self.last_processed_timestamp = None
        self.current_candle = {
            'open': None,
            'high': None,
            'low': None,
            'close': None,
            'timestamp': None
        }
        self.candles = []  # Store historical candles
        self.cooldown_until = None  # Timestamp until when dip detection is disabled
        self.dip_already_triggered = False  # Flag to track if a dip has been triggered in this session
        self.processed_candle_timestamps = set()  # Track which candle timestamps we've already processed
        
        # Try to load historical candles from the database on initialization
        self.load_candles_from_db()
        
    def parse_timestamp(self, timestamp_str):
        """Parse timestamp string to datetime object, handling different formats."""
        if not timestamp_str:
            logging.error("Empty timestamp string provided to parse_timestamp")
            return None
            
        try:
            # Try standard format first
            return datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            try:
                # Try with timezone info (like '2025-04-26 22:42:56+00:00')
                if '+' in timestamp_str:
                    # Parse with timezone aware format
                    from datetime import timezone
                    dt = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S%z')
                    # Convert to UTC and remove timezone info for consistency
                    return dt.astimezone(timezone.utc).replace(tzinfo=None)
                else:
                    # Just remove timezone part and try again
                    ts_parts = timestamp_str.split('+')[0].strip()
                    return datetime.datetime.strptime(ts_parts, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    # Try alternative format
                    return datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')
                except ValueError:
                    try:
                        # Another alternative format
                        return datetime.datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S')
                    except Exception as e:
                        logging.error(f"Error parsing timestamp {timestamp_str}: {e}")
                        return None
    
    def fetch_latest_data(self, conn=None):
        """Fetch latest data from SQLite Summary table."""
        close_conn = False
        try:
            if conn is None:
                conn = sqlite3.connect(self.db_path)
                close_conn = True
                
            cursor = conn.cursor()
            
            # Get the latest data from Summary table
            cursor.execute('''
                SELECT Timestamp, WeightedSharePrice
                FROM Summary
                ORDER BY id DESC
                LIMIT 1
            ''')
            
            result = cursor.fetchone()
            
            if result:
                timestamp = self.parse_timestamp(result[0])
                price = result[1]
                logging.info(f"Fetched latest data: timestamp={result[0]}, price={price}")
                return timestamp, price
            
            logging.warning("No data found in Summary table")
            return None, None
            
        except sqlite3.Error as e:
            logging.error(f"Error fetching latest data: {e}\n{traceback.format_exc()}")
            return None, None
        finally:
            if close_conn and conn:
                conn.close()
    
    def _get_candle_start_time(self, timestamp):
        """Get the start time of the candle that this timestamp belongs to."""
        if timestamp is None:
            return None
            
        # Remove timezone info if present to ensure consistent handling
        if hasattr(timestamp, 'tzinfo') and timestamp.tzinfo is not None:
            # Convert to UTC if it's not already
            timestamp = timestamp.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            
        minutes = timestamp.minute
        candle_number = minutes // self.interval_minutes
        
        # Calculate the beginning of the current candle period
        candle_start = timestamp.replace(
            minute=candle_number * self.interval_minutes,
            second=0,
            microsecond=0
        )
        
        return candle_start
    
    def get_existing_candle(self, timestamp_str):
        """Get existing candle data from database if it exists."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT Timestamp, Open, High, Low, Close 
                FROM OHLC 
                WHERE Timestamp = ?
            ''', (timestamp_str,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    'timestamp': self.parse_timestamp(result[0]),
                    'open': result[1],
                    'high': result[2],
                    'low': result[3],
                    'close': result[4]
                }
            return None
        except Exception as e:
            logging.error(f"Error checking for existing candle: {e}\n{traceback.format_exc()}")
            return None
    
    def update_candle(self, timestamp, price):
        """Update the current OHLC candle with the latest data."""
        if price is None or timestamp is None:
            logging.warning(f"Cannot update candle: price={price}, timestamp={timestamp}")
            return None
        
        # Log the raw input for debugging
        logging.info(f"update_candle called with timestamp={timestamp.strftime('%Y-%m-%d %H:%M:%S')}, price={price}")
        
        # Get the standardized candle start time for this timestamp
        candle_start_time = self._get_candle_start_time(timestamp)
        if candle_start_time is None:
            logging.warning(f"Could not determine candle start time for timestamp: {timestamp}")
            return None
        
        # Log candle start time for debugging
        logging.info(f"Determined candle start time: {candle_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Initialize a new candle if needed
        if (self.current_candle['timestamp'] is None or 
            candle_start_time != self._get_candle_start_time(self.current_candle['timestamp'])):
            
            if self.current_candle['open'] is not None:
                # Close the previous candle and add it to history
                completed_candle = dict(self.current_candle)
                self.candles.append(completed_candle)
                
                # Keep only the last 30 candles in memory (for efficiency)
                if len(self.candles) > 30:
                    self.candles = self.candles[-30:]
                
                logging.info(f"Closing candle: Start={self.current_candle['timestamp'].strftime('%Y-%m-%d %H:%M:%S')} "
                             f"O:{self.current_candle['open']:.2f} H:{self.current_candle['high']:.2f} "
                             f"L:{self.current_candle['low']:.2f} C:{self.current_candle['close']:.2f}")
                
                # Always store the completed candle in the database
                self.store_new_candle_in_db(completed_candle)
                candle_timestamp_str = completed_candle['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                self.processed_candle_timestamps.add(candle_timestamp_str)
            
            # Check if we already have data for this new candle period
            candle_timestamp_str = candle_start_time.strftime('%Y-%m-%d %H:%M:%S')
            existing_candle = self.get_existing_candle(candle_timestamp_str)
            
            if existing_candle:
                # Use existing candle data as starting point
                self.current_candle = existing_candle
                # Update with the latest price
                self.current_candle['high'] = max(self.current_candle['high'], price)
                self.current_candle['low'] = min(self.current_candle['low'], price)
                self.current_candle['close'] = price
                logging.info(f"Updating existing candle at {candle_start_time.strftime('%Y-%m-%d %H:%M:%S')} with price {price:.2f}")
            else:
                # Start a new candle
                self.current_candle = {
                    'open': price,
                    'high': price,
                    'low': price,
                    'close': price,
                    'timestamp': candle_start_time
                }
                logging.info(f"Starting new candle at {candle_start_time.strftime('%Y-%m-%d %H:%M:%S')} with opening price {price:.2f}")
                
                # Store the new candle immediately
                self.store_new_candle_in_db(self.current_candle)
                self.processed_candle_timestamps.add(candle_timestamp_str)
            
            # Check if cooldown period should end with the new candle
            if self.cooldown_until and candle_start_time > self.cooldown_until:
                logging.info("Cooldown period ended with new candle")
                self.cooldown_until = None
        else:
            # Update the current candle with new price data
            self.current_candle['high'] = max(self.current_candle['high'], price)
            self.current_candle['low'] = min(self.current_candle['low'], price)
            self.current_candle['close'] = price
            
            # Store updates to the current candle in the database
            self.store_new_candle_in_db(self.current_candle)
            
            # Log the update for debugging
            logging.info(f"Updated current candle: O:{self.current_candle['open']:.2f} "
                        f"H:{self.current_candle['high']:.2f} L:{self.current_candle['low']:.2f} "
                        f"C:{self.current_candle['close']:.2f}")
        
        return self.current_candle
    
    def store_new_candle_in_db(self, candle, entry_price=None, exit_price=None):
        """
        Store a completed candle in the database, updating it if it already exists.
        
        Parameters:
            candle (dict): The candle data to store
            entry_price (float, optional): The entry price if a dip was detected in this candle
            exit_price (float, optional): The exit price if a position was closed in this candle
        """
        try:
            # Create a temporary connection to the database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Format timestamp properly for storage
            timestamp_str = candle['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            
            # Check if this candle timestamp already exists
            cursor.execute("SELECT id, EntryPrice, ExitPrice FROM OHLC WHERE Timestamp = ?", (timestamp_str,))
            existing_record = cursor.fetchone()
            
            # Calculate percentages
            high_pct = ((candle['high'] - candle['low']) / candle['low']) * 100 if candle['low'] > 0 else 0
            low_pct = ((candle['high'] - candle['low']) / candle['high']) * 100 if candle['high'] > 0 else 0
            close_pct = ((candle['close'] - candle['open']) / candle['open']) * 100 if candle['open'] > 0 else 0
            
            # If we have a new entry/exit price but there's existing data, we should preserve the old values
            if existing_record:
                existing_id, existing_entry, existing_exit = existing_record
                
                # Only update entry/exit prices if they're not already set or if new ones are provided
                if entry_price is None:
                    entry_price = existing_entry
                if exit_price is None:
                    exit_price = existing_exit
                
                # Candle already exists - update it with the latest data
                cursor.execute('''
                    UPDATE OHLC 
                    SET High = MAX(High, ?), 
                        Low = MIN(Low, ?),
                        Close = ?,
                        HighPct = ?,
                        LowPct = ?,
                        ClosePct = ?,
                        EntryPrice = ?,
                        ExitPrice = ?,
                        CandlePeriod = ?
                    WHERE id = ?
                ''', (
                    candle['high'],
                    candle['low'],
                    candle['close'],
                    high_pct,
                    low_pct,
                    close_pct,
                    entry_price,
                    exit_price,
                    self.interval_minutes,
                    existing_id
                ))
                logging.info(f"Updated existing OHLC candle for {timestamp_str} in database")
            else:
                # Insert new candle record
                cursor.execute('''
                    INSERT INTO OHLC (Timestamp, Open, High, Low, Close, HighPct, LowPct, ClosePct, EntryPrice, ExitPrice, CandlePeriod)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    timestamp_str,
                    candle['open'],
                    candle['high'],
                    candle['low'],
                    candle['close'],
                    high_pct,
                    low_pct,
                    close_pct,
                    entry_price,
                    exit_price,
                    self.interval_minutes
                ))
                logging.info(f"Stored new OHLC candle for {timestamp_str} in database")
            
            conn.commit()
            conn.close()
        except Exception as e:
            logging.error(f"Error storing OHLC candle in database: {e}\n{traceback.format_exc()}")
    
    def check_dip_threshold(self):
        """Check if the current candle has a dip greater than the threshold percentage."""
        # Check if we're in a cooldown period
        if self.cooldown_until is not None and datetime.datetime.now() < self.cooldown_until:
            logging.info(f"In cooldown period until {self.cooldown_until.strftime('%H:%M:%S')}, skipping dip check")
            return False
            
        # Check if a dip has already been triggered in this trading session
        if self.dip_already_triggered:
            logging.info("Dip already triggered in this session, no more dip buying allowed")
            return False
            
        if self.current_candle['open'] is None:
            return False
            
        high = self.current_candle['high']
        low = self.current_candle['low']
        
        if high == 0:  # Avoid division by zero
            return False
            
        dip_percentage = ((high - low) / high) * 100
        
        if dip_percentage >= DIP_THRESHOLD_PERCENTAGE:
            logging.info(f"Dip threshold detected: {dip_percentage:.2f}% (threshold: {DIP_THRESHOLD_PERCENTAGE}%)")
            self.dip_already_triggered = True  # Mark that a dip has been triggered in this session
            return True
        
        return False
    
    def get_current_candle(self):
        """Get the current candle."""
        return self.current_candle
    
    def get_candles(self):
        """Get all stored candles."""
        return self.candles
        
    def set_cooldown_until_next_candle(self):
        """Set cooldown until the end of the current candle."""
        if self.current_candle['timestamp'] is not None:
            # Get current timestamp
            now = datetime.datetime.now()
            
            # Calculate when the current candle ends
            current_candle_start = self._get_candle_start_time(now)
            next_candle_start = current_candle_start + datetime.timedelta(minutes=self.interval_minutes)
            
            self.cooldown_until = next_candle_start
            logging.info(f"Cooldown set until start of next candle: {next_candle_start.strftime('%H:%M:%S')}")
        else:
            # If no current candle, set cooldown based on current time
            now = datetime.datetime.now()
            current_candle_start = self._get_candle_start_time(now)
            next_candle_start = current_candle_start + datetime.timedelta(minutes=self.interval_minutes)
            
            self.cooldown_until = next_candle_start
            logging.info(f"No current candle, cooldown set until {next_candle_start.strftime('%H:%M:%S')}")
    
    def load_candles_from_db(self):
        """Load most recent candles from the database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if OHLC table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='OHLC'")
            if not cursor.fetchone():
                logging.info("OHLC table doesn't exist yet, skipping candle loading")
                conn.close()
                return
                
            # Count rows in OHLC table
            cursor.execute("SELECT COUNT(*) FROM OHLC")
            count = cursor.fetchone()[0]
            logging.info(f"Found {count} OHLC candles in database")
            
            if count == 0:
                # No candles in database - check if we can create an initial candle from Summary data
                logging.info("No OHLC candles in database - checking if we can create an initial candle from Summary")
                cursor.execute("SELECT Timestamp, WeightedSharePrice FROM Summary ORDER BY id DESC LIMIT 1")
                result = cursor.fetchone()
                
                if result:
                    timestamp_str, price = result
                    timestamp = self.parse_timestamp(timestamp_str)
                    
                    if timestamp and price:
                        candle_start_time = self._get_candle_start_time(timestamp)
                        if candle_start_time:
                            logging.info(f"Creating initial OHLC candle from Summary data: {price} at {candle_start_time}")
                            
                            # Create initial candle
                            cursor.execute('''
                                INSERT INTO OHLC (Timestamp, Open, High, Low, Close, CandlePeriod)
                                VALUES (?, ?, ?, ?, ?, ?)
                            ''', (
                                candle_start_time.strftime('%Y-%m-%d %H:%M:%S'),
                                price,
                                price,
                                price,
                                price,
                                self.interval_minutes
                            ))
                            conn.commit()
                            
                            # Initialize current candle
                            self.current_candle = {
                                'timestamp': candle_start_time,
                                'open': price,
                                'high': price,
                                'low': price,
                                'close': price
                            }
                            logging.info(f"Created initial OHLC candle at {candle_start_time.strftime('%H:%M')} with price {price}")
            
            # Get up to 30 most recent candles ordered by timestamp (newest first)
            cursor.execute('''
                SELECT Timestamp, Open, High, Low, Close, HighPct, LowPct, ClosePct, EntryPrice, ExitPrice, CandlePeriod
                FROM OHLC
                ORDER BY datetime(Timestamp) DESC, id DESC
                LIMIT 30
            ''')
            
            results = cursor.fetchall()
            loaded_candles = []
            most_recent_candle = None
            most_recent_timestamp = None
            
            for row in results:
                timestamp = self.parse_timestamp(row[0])
                if timestamp is None:
                    continue
                    
                candle = {
                    'timestamp': timestamp,
                    'open': row[1],
                    'high': row[2],
                    'low': row[3],
                    'close': row[4],
                    'high_pct': row[5],
                    'low_pct': row[6],
                    'close_pct': row[7],
                    'entry_price': row[8],
                    'exit_price': row[9],
                    'period': row[10]
                }
                
                # Track this timestamp as already processed
                self.processed_candle_timestamps.add(row[0])
                
                # Store the most recent candle
                if most_recent_timestamp is None or timestamp > most_recent_timestamp:
                    most_recent_timestamp = timestamp
                    most_recent_candle = candle
                
                loaded_candles.append(candle)
            
            # We want to work only with the most recent data, so we don't need to reverse
            # the loaded_candles list anymore. This will make us start from the newest data.
            
            if loaded_candles:
                self.candles = loaded_candles
                logging.info(f"Loaded {len(loaded_candles)} most recent candles from database (newest first)")
                
                # Set current candle to the actual most recent one (from database) if it's still active
                if most_recent_candle:
                    latest_candle_end = most_recent_candle['timestamp'] + datetime.timedelta(minutes=self.interval_minutes)
                    
                    if datetime.datetime.now() < latest_candle_end:
                        self.current_candle = dict(most_recent_candle)
                        logging.info(f"Resumed current active candle from {most_recent_candle['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
                        
                    # Log the most recent candle information for debugging
                    logging.info(f"Most recent candle in DB: timestamp={most_recent_candle['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}, "
                                f"open={most_recent_candle['open']}, high={most_recent_candle['high']}, "
                                f"low={most_recent_candle['low']}, close={most_recent_candle['close']}")
            else:
                logging.info("No historical candles found in database")
            
            conn.close()
        except Exception as e:
            logging.error(f"Error loading candles from database: {e}\n{traceback.format_exc()}")
    
    def reset_for_new_session(self):
        """Reset the dip trigger flag for a new trading session."""
        self.dip_already_triggered = False
        self.cooldown_until = None
        logging.info("OHLC calculator reset for new trading session")


class SignalHandler:
    """Handles signal file changes for individual positions during active mirroring"""
    def __init__(self, db_path):
        self.db_path = db_path
        self.active = False
        self.watched_signals = {}  # Dictionary to track signal files and their last values
        
    def start_monitoring(self, signal_files):
        """Start monitoring a list of signal files for changes"""
        logging.info(f"Starting to monitor {len(signal_files)} signal files for direction changes")
        self.active = True
        
        # Initialize watched signals with current values
        for signal_file, initial_value in signal_files.items():
            self.watched_signals[signal_file] = initial_value
            logging.info(f"Initialized signal monitor for {signal_file} with value {initial_value}")
        
        # In a real implementation, this would set up watchdog observers for each file
        # For our test implementation, we'll just log that monitoring has started
        logging.info("Signal watchdog activated - will detect individual position changes")
        return True
        
    def stop_monitoring(self):
        """Stop monitoring signal files"""
        if self.active:
            logging.info("Stopping signal file monitoring")
            self.active = False
            self.watched_signals = {}
            # In a real implementation, this would stop the watchdog observers
            logging.info("Signal watchdog stopped")
        return True
        
    def process_signal_change(self, signal_file, new_value):
        """Process a signal change for an individual position"""
        if not self.active:
            logging.info(f"Signal change detected but monitoring is not active, ignoring")
            return False
            
        old_value = self.watched_signals.get(signal_file)
        if old_value == new_value:
            logging.info(f"No change in signal value for {signal_file}")
            return False
            
        logging.info(f"Signal change detected for {signal_file}: {old_value} -> {new_value}")
        self.watched_signals[signal_file] = new_value
        
        # Record the signal change in the database
        self.record_signal_change(signal_file, old_value, new_value)
        
        # In a real implementation, this would trigger an order
        # For our test implementation, we'll just log the action
        logging.info(f"Would place order for signal change: {signal_file} direction now {new_value}")
        return True
        
    def record_signal_change(self, signal_file, old_value, new_value):
        """Record signal change in the database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Make sure we have a table to store signal changes
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS SignalChanges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                Timestamp TEXT NOT NULL,
                SignalFile TEXT NOT NULL,
                OldValue TEXT,
                NewValue TEXT
            )
            ''')
            
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Insert signal change record
            cursor.execute('''
                INSERT INTO SignalChanges (Timestamp, SignalFile, OldValue, NewValue)
                VALUES (?, ?, ?, ?)
            ''', (timestamp, signal_file, old_value, new_value))
            
            conn.commit()
            conn.close()
            logging.info(f"Recorded signal change in database: {signal_file} {old_value} -> {new_value}")
        except Exception as e:
            logging.error(f"Error recording signal change in database: {e}\n{traceback.format_exc()}")

class PositionMirror:
    def __init__(self, db_path):
        self.db_path = db_path
        self.mirrored_positions = {}  # Dictionary to track mirrored positions
        self.entry_price = None  # WeightedSharePrice at time of mirroring
        self.exit_price = None  # WeightedSharePrice at time of exit
        self.mirroring_active = False  # Flag to track if mirroring has been activated
        self.signal_handler = SignalHandler(db_path)  # Handler for signal file changes
        
    def mirror_positions(self, current_price, signal_files=None):
        """Mirror positions and set entry price to current WeightedSharePrice."""
        # Set entry price for portfolio-wide TP/SL
        self.entry_price = current_price
        self.exit_price = None
        self.mirroring_active = True
        
        logging.info(f"Position mirroring activated at entry price: {self.entry_price:.2f}")
        logging.info(f"Portfolio TP set at: {self.entry_price * (1 + TAKE_PROFIT_PERCENTAGE/100):.2f}")
        logging.info(f"Portfolio SL set at: {self.entry_price * (1 - STOP_LOSS_PERCENTAGE/100):.2f}")
        
        # Record entry in database
        self.record_entry_in_db(current_price)
        
        # Set up signal file monitoring if signal files are provided
        if signal_files:
            # In a real implementation, this would be fetched from actual files
            # For our test implementation, we'll use a dictionary
            if not isinstance(signal_files, dict):
                # If not provided as a dict, create a dummy dict with "BUY" values
                signal_files_dict = {f"signal_file_{i}": "BUY" for i in range(len(signal_files))}
            else:
                signal_files_dict = signal_files
                
            # Start monitoring signal files for changes
            self.signal_handler.start_monitoring(signal_files_dict)
        
        return True
    
    def record_entry_in_db(self, price):
        """Record entry price in the database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Make sure we have a table to store mirror entries/exits
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS MirrorTrades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                Timestamp TEXT NOT NULL,
                EntryPrice REAL,
                ExitPrice REAL,
                ExitReason TEXT,
                Status TEXT
            )
            ''')
            
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Insert new mirror trade with entry price
            cursor.execute('''
                INSERT INTO MirrorTrades (Timestamp, EntryPrice, Status)
                VALUES (?, ?, ?)
            ''', (timestamp, price, "OPEN"))
            
            conn.commit()
            conn.close()
            logging.info(f"Recorded mirror entry in database at price {price:.2f}")
        except Exception as e:
            logging.error(f"Error recording mirror entry in database: {e}\n{traceback.format_exc()}")
    
    def record_exit_in_db(self, price, reason):
        """Record exit price in the database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get the most recent open mirror trade
            cursor.execute('''
                SELECT id FROM MirrorTrades 
                WHERE Status = 'OPEN'
                ORDER BY id DESC
                LIMIT 1
            ''')
            
            trade_id = cursor.fetchone()
            
            if trade_id:
                # Update the trade with exit price and reason
                cursor.execute('''
                    UPDATE MirrorTrades
                    SET ExitPrice = ?, ExitReason = ?, Status = 'CLOSED'
                    WHERE id = ?
                ''', (price, reason, trade_id[0]))
                
                conn.commit()
                conn.close()
                logging.info(f"Recorded mirror exit in database at price {price:.2f}, reason: {reason}")
            else:
                logging.warning("No open mirror trade found to record exit")
                conn.close()
        except Exception as e:
            logging.error(f"Error recording mirror exit in database: {e}\n{traceback.format_exc()}")
    
    def check_portfolio_tp_sl(self, current_price):
        """Check if the portfolio-wide take profit or stop loss has been hit"""
        if not self.mirroring_active or self.entry_price is None:
            return False, None
        
        # Calculate percentage change from entry
        percent_change = ((current_price - self.entry_price) / self.entry_price) * 100
        
        if percent_change >= TAKE_PROFIT_PERCENTAGE:
            logging.info(f"Portfolio take profit hit: Current price {current_price:.2f} is {percent_change:.2f}% above entry price {self.entry_price:.2f}")
            self.exit_price = current_price
            self.mirroring_active = False
            self.record_exit_in_db(current_price, "TAKE_PROFIT")
            return True, "TAKE_PROFIT"
        elif percent_change <= -STOP_LOSS_PERCENTAGE:
            logging.info(f"Portfolio stop loss hit: Current price {current_price:.2f} is {abs(percent_change):.2f}% below entry price {self.entry_price:.2f}")
            self.exit_price = current_price
            self.mirroring_active = False
            self.record_exit_in_db(current_price, "STOP_LOSS")
            return True, "STOP_LOSS"
        
        return False, None
    
    def clear_positions(self):
        """Clear all mirrored positions and reset entry price"""
        self.mirrored_positions = {}
        self.entry_price = None
        self.exit_price = None
        self.mirroring_active = False
        
        # Stop signal file monitoring
        self.signal_handler.stop_monitoring()
        
        logging.info("Cleared all mirrored positions and stopped signal monitoring")

class DataSyncManager:
    def __init__(self):
        self.config = Config()
        self.bq_client = BigQueryClient(self.config)
        self.db = SQLiteDatabase(self.config)
        self.ohlc_calculator = FixedOHLCCalculator(self.config.db_path)
        self.position_mirror = PositionMirror(self.config.db_path)
        self.running = False
        
        # Initialize OHLC calculator for new session
        logging.info("Initializing new trading session - resetting OHLC calculator")
        self.ohlc_calculator.reset_for_new_session()
    
    def run(self):
        """Run the data synchronization process."""
        self.running = True
        logging.info("Starting data synchronization process")
        
        try:
            while self.running:
                start_time = time.time()
                
                # Fetch only summary data from BigQuery
                summary_df = self.bq_client.fetch_summary()
                
                # Store only summary data in SQLite
                summary_count = self.db.store_summary(summary_df)
                
                # Set empty position and order counts for logging
                positions_count = 0
                open_orders_count = 0
                
                # Update OHLC candles with latest data
                try:
                    timestamp, price = self.ohlc_calculator.fetch_latest_data(self.db.conn)
                    if timestamp and price:
                        # Update OHLC candle 
                        current_candle = self.ohlc_calculator.update_candle(timestamp, price)
                        
                        # Check for dip threshold and that mirroring is not already active
                        if self.ohlc_calculator.check_dip_threshold() and not self.position_mirror.mirroring_active:
                            # Log that a dip has been detected
                            logging.info("Dip threshold met! Starting position mirroring")
                            
                            # Get WeightedSharePrice to use as entry price
                            current_price = price
                            
                            # Create dummy signal files for testing purposes
                            signal_files = {
                                "strategy_one.py": "BUY",
                                "strategy_two.py": "BUY",
                                "strategy_three.py": "SELL"
                            }
                            
                            # Mirror positions with current price as entry point and start signal monitoring
                            self.position_mirror.mirror_positions(current_price, signal_files)
                            
                            # Store the entry price in the current OHLC candle
                            self.ohlc_calculator.store_new_candle_in_db(current_candle, entry_price=current_price)
                            logging.info(f"Updated OHLC candle with entry price: {current_price:.2f}")
                        
                        # Check if take profit or stop loss has been hit
                        elif self.position_mirror.mirroring_active:
                            tp_sl_hit, reason = self.position_mirror.check_portfolio_tp_sl(price)
                            
                            if tp_sl_hit:
                                # Exit positions and set cooldown
                                logging.info(f"Portfolio {reason} hit. Exiting all positions.")
                                
                                # Clear mirrored positions
                                self.position_mirror.clear_positions()
                                
                                # Update the current OHLC candle with the exit price
                                self.ohlc_calculator.store_new_candle_in_db(current_candle, exit_price=price)
                                logging.info(f"Updated OHLC candle with exit price: {price:.2f}")
                                
                                # Set cooldown until the end of the current candle
                                self.ohlc_calculator.set_cooldown_until_next_candle()
                                logging.info("Cooldown set after portfolio exit to prevent re-entry in same candle")
                    else:
                        logging.warning("Could not fetch latest timestamp and price for OHLC update")
                except Exception as ohlc_error:
                    logging.error(f"Error updating OHLC candle: {ohlc_error}\n{traceback.format_exc()}")
                
                # Log current candle info
                try:
                    current_candle = self.ohlc_calculator.get_current_candle()
                    if current_candle['open'] is not None:
                        logging.info(f"Current {CANDLE_PERIOD_MINUTES}m OHLC: O:{current_candle['open']:.2f} H:{current_candle['high']:.2f} L:{current_candle['low']:.2f} C:{current_candle['close']:.2f}")
                        
                        # Simulate signal file changes if mirroring is active
                        if self.position_mirror.mirroring_active and random.random() < 0.05:  # 5% chance each cycle
                            # Randomly select a signal file to change
                            if self.position_mirror.signal_handler.watched_signals:
                                signal_file = random.choice(list(self.position_mirror.signal_handler.watched_signals.keys()))
                                current_value = self.position_mirror.signal_handler.watched_signals.get(signal_file)
                                new_value = "SELL" if current_value == "BUY" else "BUY"
                                
                                # Process the signal change
                                self.position_mirror.signal_handler.process_signal_change(signal_file, new_value)
                                logging.info(f"Simulated signal change for {signal_file}: {current_value} -> {new_value}")
                except Exception as log_error:
                    logging.error(f"Error logging current candle: {log_error}")
                
                # Log the results
                sync_time = time.time() - start_time
                logging.info(f"Sync completed in {sync_time:.2f}s - Positions: {positions_count}, Orders: {open_orders_count}, Summary: {summary_count}")
                
                # Wait for the next sync interval
                sleep_time = max(1, self.config.sync_interval - sync_time)
                time.sleep(sleep_time)
        except KeyboardInterrupt:
            logging.info("Data synchronization process interrupted")
        except Exception as e:
            logging.error(f"Error in data synchronization process: {e}\n{traceback.format_exc()}")
        finally:
            # Make sure to store the final candle state before exiting
            try:
                current_candle = self.ohlc_calculator.get_current_candle()
                if current_candle['open'] is not None:
                    logging.info("Storing final candle state before exit")
                    self.ohlc_calculator.store_new_candle_in_db(current_candle)
            except Exception as final_error:
                logging.error(f"Error storing final candle: {final_error}")
                
            self.db.close()
            logging.info("Data synchronization process ended")
    
    def stop(self):
        """Stop the data synchronization process."""
        self.running = False
        logging.info("Stopping data synchronization process")


if __name__ == "__main__":
    try:
        sync_manager = DataSyncManager()
        sync_manager.run()
    except KeyboardInterrupt:
        logging.info("Program interrupted by user")
    except Exception as e:
        logging.error(f"Unhandled exception: {e}\n{traceback.format_exc()}")
    finally:
        logging.info("Program ended")