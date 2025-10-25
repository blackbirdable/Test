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
    
    def fetch_positions(self, limit=MAX_ROWS_TO_KEEP):
        """Fetch recent position data from BigQuery."""
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
    
    def fetch_open_orders(self, limit=MAX_ROWS_TO_KEEP):
        """Fetch recent open orders data from BigQuery."""
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
    
    def fetch_summary(self, limit=MAX_ROWS_TO_KEEP):
        """Fetch recent summary data from BigQuery."""
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
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            logging.info("SQLite database connection closed")


class OHLCCalculator:
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
                timestamp = datetime.datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S')
                price = result[1]
                return timestamp, price
            
            return None, None
            
        except sqlite3.Error as e:
            logging.error(f"Error fetching latest data: {e}\n{traceback.format_exc()}")
            return None, None
        finally:
            if close_conn and conn:
                conn.close()
    
    def _get_candle_start_time(self, timestamp):
        """Get the start time of the candle that this timestamp belongs to.
        For example, with 25-minute candles, timestamps 13:27 and 13:49 would both
        return 13:25 as the candle start time."""
        minutes = timestamp.minute
        candle_number = minutes // self.interval_minutes
        
        # Calculate the beginning of the current candle period
        candle_start = timestamp.replace(
            minute=candle_number * self.interval_minutes,
            second=0,
            microsecond=0
        )
        
        return candle_start
    
    def update_candle(self, timestamp, price):
        """Update the current OHLC candle with the latest data."""
        if price is None:
            return None
        
        # Get the standardized candle start time for this timestamp
        candle_start_time = self._get_candle_start_time(timestamp)
        
        # Initialize a new candle if needed
        if self.current_candle['timestamp'] is None or candle_start_time != self._get_candle_start_time(self.current_candle['timestamp']):
            if self.current_candle['open'] is not None:
                # Close the previous candle and add it to history
                self.candles.append(dict(self.current_candle))
                
                # Keep only the last 30 candles (for efficiency)
                if len(self.candles) > 30:
                    self.candles = self.candles[-30:]
                
                logging.info(f"Closing candle: Start={self.current_candle['timestamp'].strftime('%H:%M')} O:{self.current_candle['open']:.2f} H:{self.current_candle['high']:.2f} L:{self.current_candle['low']:.2f} C:{self.current_candle['close']:.2f}")
            
            # Start a new candle
            self.current_candle = {
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'timestamp': candle_start_time  # Use the standard candle start time
            }
            
            logging.info(f"Starting new candle at {candle_start_time.strftime('%H:%M')} with opening price {price:.2f}")
            
            # Check if cooldown period should end with the new candle
            if self.cooldown_until and candle_start_time > self.cooldown_until:
                logging.info("Cooldown period ended with new candle")
                self.cooldown_until = None
        else:
            # Update the current candle
            self.current_candle['high'] = max(self.current_candle['high'], price)
            self.current_candle['low'] = min(self.current_candle['low'], price)
            self.current_candle['close'] = price
        
        return self.current_candle
    
    def check_dip_threshold(self):
        """Check if the current candle has a dip greater than the threshold percentage"""
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
        """Set cooldown until the end of the current candle"""
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
    
    def reset_for_new_session(self):
        """Reset the dip trigger flag for a new trading session"""
        self.dip_already_triggered = False
        self.cooldown_until = None
        logging.info("OHLC calculator reset for new trading session")


class DataSyncManager:
    def __init__(self):
        self.config = Config()
        self.bq_client = BigQueryClient(self.config)
        self.db = SQLiteDatabase(self.config)
        self.ohlc_calculator = OHLCCalculator(self.config.db_path)
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
                
                # Fetch data from BigQuery
                positions_df = self.bq_client.fetch_positions()
                open_orders_df = self.bq_client.fetch_open_orders()
                summary_df = self.bq_client.fetch_summary()
                
                # Store data in SQLite
                positions_count = self.db.store_positions(positions_df)
                open_orders_count = self.db.store_open_orders(open_orders_df)
                summary_count = self.db.store_summary(summary_df)
                
                # Update OHLC candles with latest data
                timestamp, price = self.ohlc_calculator.fetch_latest_data(self.db.conn)
                if timestamp and price:
                    # Update OHLC candle
                    current_candle = self.ohlc_calculator.update_candle(timestamp, price)
                    
                    # Check for dip threshold
                    if self.ohlc_calculator.check_dip_threshold():
                        # This is where you would trigger the mirroring logic
                        # For now, we'll just log that a dip has been detected
                        logging.info("Dip threshold met! Mirroring would be triggered here.")
                
                # Log current candle info
                current_candle = self.ohlc_calculator.get_current_candle()
                if current_candle['open'] is not None:
                    logging.info(f"Current {CANDLE_PERIOD_MINUTES}m OHLC: O:{current_candle['open']:.2f} H:{current_candle['high']:.2f} L:{current_candle['low']:.2f} C:{current_candle['close']:.2f}")
                
                # Log the results
                sync_time = time.time() - start_time
                logging.info(f"Sync completed in {sync_time:.2f}s - Positions: {positions_count}, Orders: {open_orders_count}, Summary: {summary_count}")
                
                # Wait for the next sync interval
                time.sleep(max(1, self.config.sync_interval - sync_time))
        except KeyboardInterrupt:
            logging.info("Data synchronization process interrupted")
        except Exception as e:
            logging.error(f"Error in data synchronization process: {e}\n{traceback.format_exc()}")
        finally:
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