import pandas as pd
from collections import deque
from ctrader_open_api import Client, Protobuf, TcpProtocol, Auth, EndPoints
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import *
from ctrader_open_api.messages.OpenApiMessages_pb2 import *
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOAOrderType, ProtoOATradeSide, ProtoOAExecutionType
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import ProtoMessage
from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOAExecutionEvent
from twisted.internet import reactor, task
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from StrategyConfigAC1 import strategies, CLIENT_ID, CLIENT_SECRET, ACCESS_TOKEN, ACCOUNT_ID
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import datetime
import asyncio
import threading
import logging
import queue
import ctypes
import importlib.util
import json
import random
import time
import socket
import sqlite3
import traceback

def set_terminal_title(title):
    ctypes.windll.kernel32.SetConsoleTitleW(title)

def log_to_sqlite(event_type, message):
    db_path = r'C:\Users\Administrator\Desktop\EMS_Log_Benchmark.db'
    max_rows = 300000
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    message TEXT NOT NULL
                )
            ''')
            
            # Insert new log entry
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute('''
                INSERT INTO logs (timestamp, event_type, message)
                VALUES (?, ?, ?)
            ''', (timestamp, event_type, message))
            
            # Check if we need to clean up old records
            cursor.execute('SELECT COUNT(*) FROM logs')
            row_count = cursor.fetchone()[0]
            
            if row_count > max_rows:
                # Delete oldest records, keeping only the most recent max_rows
                rows_to_delete = row_count - max_rows
                cursor.execute('''
                    DELETE FROM logs 
                    WHERE id IN (
                        SELECT id FROM logs 
                        ORDER BY id ASC 
                        LIMIT ?
                    )
                ''', (rows_to_delete,))
                
                # Optional: VACUUM to reclaim space (uncomment if you want to optimize storage)
                # Note: VACUUM can be slow on large databases, so use sparingly
                if rows_to_delete > 10000:  # Only vacuum if we deleted a significant number of rows
                    cursor.execute('VACUUM')
                
            
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Error logging to SQLite: {e}")

def send_request_with_timeout(client, request):
    deferred = client.send(request)
    deferred.addTimeout(DEFERRED_TIMEOUT, reactor)
    return deferred

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DEFERRED_TIMEOUT = 3000  # Global timeout for Twisted Deferreds in seconds
SIGNAL_DELAY_SECONDS = 3  # Global delay for signal processing

# BigQuery configuration
credentials_path = r"C:\Users\Administrator\Desktop\Sonixen\Tools\Ctrader OpenAPI\massive-clone-400221-a39952ec190a.json"
project_id = "massive-clone-400221"
dataset_id = "benchmark_portfolio"

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
            logging.info(f"Dataset {dataset_id} already exists")
        except Exception:
            dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
            dataset.location = "US"
            dataset = client.create_dataset(dataset, timeout=30)
            logging.info(f"Created dataset {dataset_id}")
        
        # Create EMS-BENCHMARK table if it doesn't exist
        create_ems_benchmark_table(client)
        
        return client
    except Exception as e:
        logging.error(f"Error initializing BigQuery client: {e}")
        log_to_sqlite('ERROR', f"BigQuery client initialization error: {e}\n{traceback.format_exc()}")
        raise

def create_ems_benchmark_table(client):
    """Create EMS-BENCHMARK table if it doesn't exist."""
    ems_benchmark_schema = [
        bigquery.SchemaField("Timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("EventType", "STRING"),
        bigquery.SchemaField("StrategyLabel", "STRING"),
        bigquery.SchemaField("Signal", "STRING"),
        bigquery.SchemaField("Symbol", "STRING"),
        bigquery.SchemaField("Volume", "FLOAT"),
        bigquery.SchemaField("EntryPrice", "FLOAT"),
        bigquery.SchemaField("StopLossPrice", "FLOAT"),
        bigquery.SchemaField("TakeProfitPrice", "FLOAT"),
        bigquery.SchemaField("OrderId", "STRING"),
        bigquery.SchemaField("PositionId", "STRING"),
        bigquery.SchemaField("ExecutionType", "STRING"),
        bigquery.SchemaField("Status", "STRING"),
        bigquery.SchemaField("ErrorMessage", "STRING"),
        bigquery.SchemaField("BestBid", "FLOAT"),
        bigquery.SchemaField("BestAsk", "FLOAT"),
        bigquery.SchemaField("QuotesLive", "BOOLEAN"),
        bigquery.SchemaField("AdditionalData", "STRING")
    ]
    
    table_id = f"{project_id}.{dataset_id}.EMS_BENCHMARK"
    try:
        client.get_table(table_id)
        logging.info("Table EMS_BENCHMARK already exists")
    except Exception:
        table = bigquery.Table(table_id, schema=ems_benchmark_schema)
        table = client.create_table(table)
        logging.info(f"Created table {table.table_id}")

def log_to_bigquery(bq_client, event_type, strategy_label=None, signal=None, symbol=None, 
                   volume=None, entry_price=None, stop_loss_price=None, take_profit_price=None,
                   order_id=None, position_id=None, execution_type=None, status=None, 
                   error_message=None, best_bid=None, best_ask=None, quotes_live=None, 
                   additional_data=None):
    """Log event data to BigQuery EMS_BENCHMARK table."""
    if not bq_client:
        return
    
    try:
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        row_data = {
            "Timestamp": timestamp,
            "EventType": event_type,
            "StrategyLabel": strategy_label,
            "Signal": signal,
            "Symbol": symbol,
            "Volume": float(volume) if volume is not None else None,
            "EntryPrice": float(entry_price) if entry_price is not None else None,
            "StopLossPrice": float(stop_loss_price) if stop_loss_price is not None else None,
            "TakeProfitPrice": float(take_profit_price) if take_profit_price is not None else None,
            "OrderId": str(order_id) if order_id is not None else None,
            "PositionId": str(position_id) if position_id is not None else None,
            "ExecutionType": execution_type,
            "Status": status,
            "ErrorMessage": error_message,
            "BestBid": float(best_bid) if best_bid is not None else None,
            "BestAsk": float(best_ask) if best_ask is not None else None,
            "QuotesLive": quotes_live,
            "AdditionalData": json.dumps(additional_data) if additional_data else None
        }
        
        table_id = f"{project_id}.{dataset_id}.EMS_BENCHMARK"
        errors = bq_client.insert_rows_json(table_id, [row_data])
        
        if errors:
            logging.error(f"Errors occurred while inserting EMS data to BigQuery: {errors}")
        
    except Exception as e:
        logging.error(f"Error logging to BigQuery: {e}")

class SignalTracker:
    def __init__(self, file_path='pending_signals_AC1.json', bq_client=None):
        self.file_path = file_path
        self.bq_client = bq_client
        if not os.path.exists(file_path):
            with open(file_path, 'w') as f:
                json.dump({}, f)

    def _read_file(self):
        with open(self.file_path, 'r') as f:
            return json.load(f)

    def _write_file(self, data):
        with open(self.file_path, 'w') as f:
            json.dump(data, f)

    def add_signal(self, label, signal):
        data = self._read_file()
        timestamp = datetime.datetime.now().isoformat()
        data[label] = {
            'signal': signal,
            'timestamp': timestamp,
            'status': 'pending_confirmation',
            'confirmation_time': None
        }
        self._write_file(data)
        logging.info(f"Added signal to tracker for {label}: {signal}")
        log_to_sqlite('INFO', f"Added signal to tracker for {label}: {signal}")
        log_to_bigquery(self.bq_client, 'SIGNAL_TRACKED', label, signal, status='pending_confirmation')

    def get_pending_signals(self):
        data = self._read_file()
        return {k: v for k, v in data.items() if v['status'] == 'pending_confirmation'}

    def confirm_signal(self, label, confirmed_signal):
        data = self._read_file()
        if label in data:
            data[label]['status'] = 'confirmed'
            data[label]['confirmed_signal'] = confirmed_signal
            data[label]['confirmation_time'] = datetime.datetime.now().isoformat()
            self._write_file(data)
            logging.info(f"Signal confirmed for {label}: {confirmed_signal}")
            log_to_sqlite('INFO', f"Signal confirmed for {label}: {confirmed_signal}")
            log_to_bigquery(self.bq_client, 'SIGNAL_CONFIRMED', label, confirmed_signal, status='confirmed')
            return True
        return False

    def remove_signal(self, label):
        data = self._read_file()
        if label in data:
            del data[label]
            self._write_file(data)
            logging.info(f"Removed signal from tracker for {label}")
            log_to_sqlite('INFO', f"Removed signal from tracker for {label}")
            log_to_bigquery(self.bq_client, 'SIGNAL_REMOVED_FROM_TRACKER', label, status='removed')

class SignalHandler(FileSystemEventHandler):
    def __init__(self, strategy, signal_queue, signal_tracker, bq_client=None):
        self.strategy = strategy
        self.signal_queue = signal_queue
        self.signal_tracker = signal_tracker
        self.bq_client = bq_client

    def on_modified(self, event):
        if event.src_path.endswith(os.path.basename(self.strategy['signal_file'])):
            signal = self.get_recent_signal(self.strategy['signal_file'])
            if signal is None:
                logging.warning(f"Failed to get a valid signal from {self.strategy['signal_file']}. Skipping this update.")
                log_to_sqlite('WARNING', f"Failed to get a valid signal from {self.strategy['signal_file']}. Skipping this update.")
                log_to_bigquery(self.bq_client, 'WARNING', self.strategy['label'], 
                               error_message=f"Failed to get valid signal from {self.strategy['signal_file']}")
                return
            
            if signal != self.strategy.get('last_signal'):
                logging.info(f"New signal detected for {self.strategy['label']}: {signal}. Adding to signal tracker...")
                log_to_sqlite('INFO', f"New signal detected for {self.strategy['label']}: {signal}. Adding to signal tracker...")
                log_to_bigquery(self.bq_client, 'SIGNAL_DETECTED', self.strategy['label'], signal)
                
                # Add signal to tracker for confirmation after delay
                self.signal_tracker.add_signal(self.strategy['label'], signal)

    def get_recent_signal(self, signal_file):
        try:
            spec = importlib.util.spec_from_file_location("signal_module", signal_file)
            signal_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(signal_module)
            return signal_module.Action
        except Exception as e:
            logging.error(f"Error reading signal from {signal_file}: {e}")
            log_to_sqlite('ERROR', f"Error reading signal from {signal_file}: {e}")
            return None

class OrderManager:
    def __init__(self, file_path='pending_orders_AC1.json', bq_client=None):
        self.file_path = file_path
        self.bq_client = bq_client
        if not os.path.exists(file_path):
            with open(file_path, 'w') as f:
                json.dump({}, f)

    def _read_file(self):
        with open(self.file_path, 'r') as f:
            return json.load(f)

    def _write_file(self, data):
        with open(self.file_path, 'w') as f:
            json.dump(data, f)

    def mark_order_processing(self, label):
        data = self._read_file()
        if label in data:
            data[label]['status'] = 'processing'
            self._write_file(data)
            logging.info(f"Marked order {label} as processing")
            log_to_sqlite('INFO', f"Marked order {label} as processing")
            log_to_bigquery(self.bq_client, 'ORDER_PROCESSING', label, status='processing')
        else:
            logging.warning(f"Order {label} not found for marking as processing")
            log_to_sqlite('WARNING', f"Order {label} not found for marking as processing")

    def remove_order(self, label):
        data = self._read_file()
        if label in data:
            del data[label]
            self._write_file(data)
            logging.info(f"Removed order {label}")
            log_to_sqlite('INFO', f"Removed order {label}")
            log_to_bigquery(self.bq_client, 'ORDER_REMOVED', label, status='removed')
        else:
            logging.info(f"Order {label} not found for removal, already processed")
            log_to_sqlite('INFO', f"Order {label} not found for removal, already processed")

    def add_order(self, label, order_data):
        data = self._read_file()
        order_data['status'] = 'pending'
        data[label] = order_data
        self._write_file(data)
        logging.info(f"Added new pending order {label} to pending_orders_AC1.json")
        log_to_sqlite('INFO', f"Added new pending order {label} to pending_orders_AC1.json")
        log_to_bigquery(self.bq_client, 'ORDER_ADDED', label, order_data.get('signal'), 
                       volume=order_data.get('volume'), entry_price=order_data.get('entry_price'),
                       stop_loss_price=order_data.get('stop_loss_price'), 
                       take_profit_price=order_data.get('take_profit_price'), status='pending')

    def get_pending_orders(self):
        data = self._read_file()
        return {k: v for k, v in data.items() if v['status'] == 'pending'}

    def get_processing_orders(self):
        data = self._read_file()
        return {k: v for k, v in data.items() if v['status'] == 'processing'}

# Define SocketManager class before it's used
class SocketManager:
    def __init__(self, ports, bq_client=None):
        self.ports = ports
        self.sockets = {}
        self.paused = False
        self.bq_client = bq_client

    def create_socket(self, port):
        """Create and bind a socket to a specific port."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('0.0.0.0', port))
            s.listen(5)
            logging.info(f"Socket created and listening on port {port}")
            log_to_bigquery(self.bq_client, 'SOCKET_CREATED', additional_data={'port': port})
            return s
        except socket.error as e:
            logging.error(f"Error creating socket on port {port}: {e}")
            log_to_bigquery(self.bq_client, 'SOCKET_ERROR', error_message=f"Error creating socket on port {port}: {e}")
            return None

    def close_socket(self, s, port):
        """Close and cleanup the socket."""
        try:
            s.close()
            logging.info(f"Socket on port {port} closed and cleaned up.")
            log_to_bigquery(self.bq_client, 'SOCKET_CLOSED', additional_data={'port': port})
        except Exception as e:
            logging.error(f"Error closing socket on port {port}: {e}")
            log_to_bigquery(self.bq_client, 'SOCKET_ERROR', error_message=f"Error closing socket on port {port}: {e}")

    def restart_sockets(self):
        """Restart sockets on specified ports."""
        logging.info("Restarting sockets...")
        log_to_bigquery(self.bq_client, 'SOCKET_RESTART_INITIATED')
        self.pause_order_processing()
        self.cleanup_sockets()
        self.create_sockets()
        self.resume_order_processing()
        logging.info("Sockets restarted.")
        log_to_bigquery(self.bq_client, 'SOCKET_RESTART_COMPLETED')

    def create_sockets(self):
        """Create sockets for all ports."""
        for port in self.ports:
            new_socket = self.create_socket(port)
            if new_socket:
                self.sockets[port] = new_socket

    def cleanup_sockets(self):
        """Cleanup all sockets."""
        for port, s in self.sockets.items():
            if s:
                self.close_socket(s, port)

    def pause_order_processing(self):
        """Pause order processing."""
        self.paused = True
        logging.info("Order processing paused during socket restart.")
        log_to_bigquery(self.bq_client, 'ORDER_PROCESSING_PAUSED')

    def resume_order_processing(self):
        """Resume order processing."""
        self.paused = False
        logging.info("Order processing resumed after socket restart.")
        log_to_bigquery(self.bq_client, 'ORDER_PROCESSING_RESUMED')

class TradingSystem:
    def __init__(self, strategies):
        self.client = Client(EndPoints.PROTOBUF_DEMO_HOST, EndPoints.PROTOBUF_PORT, TcpProtocol)
        self.strategies = strategies
        self.open_positions = []
        self.open_orders = []
        self.account_authorized = asyncio.Event()
        self.signal_queue = queue.Queue()
        self.observers = []
        self.loop = None
        self.bq_client = None
        self.order_manager = None
        self.signal_tracker = None
        self.reconnect_attempt = 0
        self.max_reconnect_attempts = 10
        self.base_delay = 1
        self.fresh_prices_event = asyncio.Event()
        self.socket_manager = None

        # Initialize BigQuery client
        try:
            self.bq_client = initialize_bigquery_client()
            logging.info("BigQuery client initialized successfully for EMS-BENCHMARK")
            log_to_bigquery(self.bq_client, 'SYSTEM_INITIALIZED')
        except Exception as e:
            logging.error(f"Failed to initialize BigQuery client: {e}")

        self.order_manager = OrderManager('pending_orders_AC1.json', self.bq_client)
        self.signal_tracker = SignalTracker('pending_signals_AC1.json', self.bq_client)
        self.socket_manager = SocketManager([5035, 5036], self.bq_client)

    async def wait_for_account_auth(self):
        while not self.account_authorized.is_set():
            try:
                await asyncio.wait_for(self.account_authorized.wait(), timeout=30)
            except asyncio.TimeoutError:
                logging.warning("Timed out waiting for account authorization. Retrying...")
                log_to_sqlite('WARNING', "Timed out waiting for account authorization. Retrying...")
                log_to_bigquery(self.bq_client, 'AUTH_TIMEOUT', error_message="Timed out waiting for account authorization")
                if self.client.disconnected:
                    self.connect_with_retry()

    async def retry_with_backoff(self, func, *args, max_attempts=5, base_delay=1, max_delay=60):
        for attempt in range(max_attempts):
            try:
                return await func(*args)
            except Exception as e:
                if attempt == max_attempts - 1:
                    raise
                delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
                logging.warning(f"Operation failed. Retrying in {delay:.2f} seconds. Error: {str(e)}")
                log_to_sqlite('WARNING', f"Operation failed. Retrying in {delay:.2f} seconds. Error: {str(e)}")
                log_to_bigquery(self.bq_client, 'RETRY_OPERATION', error_message=str(e), 
                               additional_data={'attempt': attempt, 'delay': delay})
                await asyncio.sleep(delay)

    async def continuous_pending_order_check(self):
        while True:
            try:
                pending_orders = self.order_manager.get_pending_orders()
                if pending_orders:
                    logging.info(f"Found {len(pending_orders)} pending orders to process")
                    log_to_bigquery(self.bq_client, 'PENDING_ORDERS_CHECK', 
                                   additional_data={'pending_count': len(pending_orders)})
                    for label, order in pending_orders.items():
                        await self.refresh_positions_and_orders()
                        
                        existing_orders = [ord for ord in self.open_orders 
                                         if ord.tradeData.label == label and 
                                         ord.tradeData.tradeSide == ProtoOATradeSide.Value(order['signal'].upper())]
                        
                        existing_positions = [pos for pos in self.open_positions 
                                            if pos.tradeData.label == label and 
                                            pos.tradeData.tradeSide == ProtoOATradeSide.Value(order['signal'].upper())]
                        
                        opposite_orders = [ord for ord in self.open_orders 
                                         if ord.tradeData.label == label and 
                                         ord.tradeData.tradeSide != ProtoOATradeSide.Value(order['signal'].upper())]
                        
                        opposite_positions = [pos for pos in self.open_positions 
                                            if pos.tradeData.label == label and 
                                            pos.tradeData.tradeSide != ProtoOATradeSide.Value(order['signal'].upper())]
                        
                        if opposite_orders or opposite_positions:
                            logging.info(f"Found opposite direction orders/positions for {label}, closing them first")
                            log_to_bigquery(self.bq_client, 'OPPOSITE_POSITIONS_FOUND', label, order['signal'])
                            await self.cancel_orders_and_positions(label)
                            continue
                        
                        if not existing_orders and not existing_positions:
                            logging.info(f"No existing order or position found for {label} in same direction, attempting to process")
                            await self.process_specific_pending_order(label, order)
                        else:
                            if existing_orders:
                                logging.info(f"Found existing order for {label} in same direction, skipping")
                                log_to_bigquery(self.bq_client, 'EXISTING_ORDER_FOUND', label, order['signal'], status='skipped')
                                self.order_manager.remove_order(label)
                            if existing_positions:
                                logging.info(f"Found existing position for {label} in same direction, skipping")
                                log_to_bigquery(self.bq_client, 'EXISTING_POSITION_FOUND', label, order['signal'], status='skipped')
                                self.order_manager.remove_order(label)
                
                await asyncio.sleep(5)
                
            except Exception as e:
                logging.error(f"Error in continuous_pending_order_check: {e}")
                log_to_bigquery(self.bq_client, 'ERROR', error_message=f"Error in continuous_pending_order_check: {e}")
                await asyncio.sleep(5)

    async def continuous_signal_confirmation_check(self):
        """Continuously check for signals that need confirmation after the delay period."""
        while True:
            try:
                pending_signals = self.signal_tracker.get_pending_signals()
                current_time = datetime.datetime.now()
                
                for label, signal_data in pending_signals.items():
                    signal_timestamp = datetime.datetime.fromisoformat(signal_data['timestamp'])
                    time_elapsed = (current_time - signal_timestamp).total_seconds()
                    
                    # Check if the delay period has passed
                    if time_elapsed >= SIGNAL_DELAY_SECONDS:
                        logging.info(f"Checking signal confirmation for {label} after {time_elapsed:.1f} seconds...")
                        log_to_sqlite('INFO', f"Checking signal confirmation for {label} after {time_elapsed:.1f} seconds...")
                        
                        # Get the current signal from the file
                        strategy = next((s for s in self.strategies if s['label'] == label), None)
                        if strategy:
                            current_signal = self.get_recent_signal(strategy['signal_file'])
                            
                            if current_signal is not None:
                                # Check if signal is different from last processed signal
                                if current_signal != strategy.get('last_signal'):
                                    # Confirm the signal and add to processing queue
                                    self.signal_tracker.confirm_signal(label, current_signal)
                                    strategy['last_signal'] = current_signal
                                    self.signal_queue.put((strategy, current_signal))
                                    
                                    logging.info(f"Signal confirmed and queued for processing: {label} -> {current_signal}")
                                    log_to_sqlite('INFO', f"Signal confirmed and queued for processing: {label} -> {current_signal}")
                                    log_to_bigquery(self.bq_client, 'SIGNAL_CONFIRMED_AND_QUEUED', label, current_signal)
                                else:
                                    # Signal reverted to previous state, no action needed
                                    logging.info(f"Signal for {label} reverted to previous state: {current_signal}, removing from tracker")
                                    log_to_sqlite('INFO', f"Signal for {label} reverted to previous state: {current_signal}, removing from tracker")
                                    log_to_bigquery(self.bq_client, 'SIGNAL_REVERTED_NO_ACTION', label, current_signal)
                                
                                # Remove from tracker either way
                                self.signal_tracker.remove_signal(label)
                            else:
                                logging.warning(f"Failed to read current signal for {label}, removing from tracker")
                                log_to_sqlite('WARNING', f"Failed to read current signal for {label}, removing from tracker")
                                self.signal_tracker.remove_signal(label)
                        else:
                            logging.warning(f"Strategy not found for label {label}, removing from tracker")
                            log_to_sqlite('WARNING', f"Strategy not found for label {label}, removing from tracker")
                            self.signal_tracker.remove_signal(label)
                
                await asyncio.sleep(0.5)  # Check every 500ms for timely signal confirmation
                
            except Exception as e:
                logging.error(f"Error in continuous_signal_confirmation_check: {e}")
                log_to_bigquery(self.bq_client, 'SIGNAL_CONFIRMATION_ERROR', error_message=str(e))
                await asyncio.sleep(5)

    def start(self):
        self.client.setConnectedCallback(self.connected)
        self.client.setDisconnectedCallback(self.disconnected)
        self.client.setMessageReceivedCallback(self.onMessageReceived)
        self.connect_with_retry()

        self.start_signal_monitoring()
        
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.run())

    async def run(self):
        self.socket_manager.create_sockets()
        asyncio.create_task(self.periodic_socket_restart(180))

        while True:
            try:
                await self.wait_for_account_auth()
                tasks = [
                    asyncio.create_task(self.process_signals()),
                    asyncio.create_task(self.continuous_pending_order_check()),
                    asyncio.create_task(self.check_fresh_quotes_for_pending_orders()),
                    asyncio.create_task(self.wait_and_process_fresh_prices()),
                    asyncio.create_task(self.continuous_signal_confirmation_check()),
                ]
                await asyncio.gather(*tasks)
            except Exception as e:
                logging.error(f"Error in main loop: {e}")
                log_to_sqlite('ERROR', f"Error in main loop: {e}")
                log_to_bigquery(self.bq_client, 'MAIN_LOOP_ERROR', error_message=str(e))
                await asyncio.sleep(5)
    
    async def periodic_socket_restart(self, interval):
        """Periodically restart the sockets every `interval` seconds."""
        while True:
            await asyncio.sleep(interval)
            logging.info("Initiating socket restart...")
            self.socket_manager.restart_sockets()
            
    def start_signal_monitoring(self):
        for strategy in self.strategies:
            event_handler = SignalHandler(strategy, self.signal_queue, self.signal_tracker, self.bq_client)
            observer = Observer()
            observer.schedule(event_handler, path=os.path.dirname(strategy['signal_file']), recursive=False)
            observer.start()
            self.observers.append(observer)

    async def process_signals(self):
        while True:
            try:
                signals = []
                while not self.signal_queue.empty():
                    signals.append(self.signal_queue.get_nowait())
                
                if signals:
                    await asyncio.gather(*[self.process_signal(signal, strategy) for strategy, signal in signals])
                else:
                    await asyncio.sleep(0.1)
            except queue.Empty:
                await asyncio.sleep(0.1)
            except Exception as e:
                logging.error(f"Error processing signals: {e}")
                log_to_sqlite('ERROR', f"Error processing signals: {e}")
                log_to_bigquery(self.bq_client, 'SIGNAL_PROCESSING_ERROR', error_message=str(e))
                await asyncio.sleep(5)

    async def process_pending_orders(self):
        try:
            logging.info("BEGIN: Processing all pending orders")
            log_to_sqlite('INFO', "BEGIN: Processing all pending orders")
            log_to_bigquery(self.bq_client, 'PENDING_ORDERS_PROCESSING_START')

            pending_orders = self.order_manager.get_pending_orders()
            if not pending_orders:
                logging.info("No pending orders to process")
                log_to_sqlite('INFO', "No pending orders to process")
                return

            for label, order in pending_orders.items():
                await self.process_specific_pending_order(label, order)

        except Exception as e:
            logging.error(f"Error in process_pending_orders: {e}")
            log_to_sqlite('ERROR', f"Error in process_pending_orders: {e}")
            log_to_bigquery(self.bq_client, 'PENDING_ORDERS_PROCESSING_ERROR', error_message=str(e))
        finally:
            logging.info("END: Processing all pending orders")
            log_to_sqlite('INFO', "END: Processing all pending orders")
            log_to_bigquery(self.bq_client, 'PENDING_ORDERS_PROCESSING_END')

    async def check_pending_orders(self):
        while True:
            await self.process_pending_orders()
            await asyncio.sleep(1)
            
    def connected(self, client):
        logging.info("Connected")
        log_to_sqlite('INFO', "Connected")
        log_to_bigquery(self.bq_client, 'CLIENT_CONNECTED')
        request = ProtoOAApplicationAuthReq()
        request.clientId = CLIENT_ID
        request.clientSecret = CLIENT_SECRET
        deferred = send_request_with_timeout(client, request)
        deferred.addCallback(self.onApplicationAuth)
        deferred.addErrback(self.onError)
        logging.info("Connected")
        self.reconnect_attempt = 0

    def onApplicationAuth(self, response):
        logging.info("API Application authorized")
        log_to_sqlite('INFO', "API Application authorized")
        log_to_bigquery(self.bq_client, 'APPLICATION_AUTHORIZED')
        self.sendProtoOAAccountAuthReq()

    def disconnected(self, client, reason):
        logging.warning(f"Disconnected: {reason}")
        log_to_sqlite('WARNING', f"Disconnected: {reason}")
        log_to_bigquery(self.bq_client, 'CLIENT_DISCONNECTED', error_message=str(reason))
        self.loop.call_soon_threadsafe(self.account_authorized.clear)
        self.connect_with_retry()

    def connect_with_retry(self):
        if self.reconnect_attempt < self.max_reconnect_attempts:
            delay = min(self.calculate_delay(), 60)
            log_message = f"Attempting to connect (attempt {self.reconnect_attempt + 1}) in {delay:.2f} seconds..."
            logging.info(log_message)
            log_to_sqlite('INFO', log_message)
            log_to_bigquery(self.bq_client, 'RECONNECT_ATTEMPT', 
                           additional_data={'attempt': self.reconnect_attempt + 1, 'delay': delay})
            
            def attempt_connection():
                try:
                    self.client.startService()
                except Exception as e:
                    error_message = f"Reconnection attempt {self.reconnect_attempt + 1} failed: {e}"
                    logging.error(error_message)
                    log_to_sqlite('ERROR', error_message)
                    log_to_bigquery(self.bq_client, 'RECONNECT_FAILED', error_message=error_message)
                    self.reconnect_attempt += 1
                    self.connect_with_retry()

            reactor.callLater(delay, attempt_connection)
        else:
            error_message = "Max reconnection attempts reached. Please check your network connection."
            logging.error(error_message)
            log_to_sqlite('ERROR', error_message)
            log_to_bigquery(self.bq_client, 'MAX_RECONNECT_REACHED', error_message=error_message)

    def calculate_delay(self):
        return self.base_delay * (2 ** self.reconnect_attempt) + (random.randint(0, 1000) / 1000.0)
            
        logging.error("Max reconnection attempts reached. Please check your network connection.")
        log_to_sqlite('ERROR', "Max reconnection attempts reached. Please check your network connection.")

    def onMessageReceived(self, client, message):
        if message.payloadType == ProtoOAAccountAuthRes().payloadType:
            protoOAAccountAuthRes = Protobuf.extract(message)
            logging.info(f"Account {protoOAAccountAuthRes.ctidTraderAccountId} has been authorized")
            log_to_sqlite('INFO', f"Account {protoOAAccountAuthRes.ctidTraderAccountId} has been authorized")
            log_to_bigquery(self.bq_client, 'ACCOUNT_AUTHORIZED', 
                           additional_data={'account_id': protoOAAccountAuthRes.ctidTraderAccountId})
            self.loop.call_soon_threadsafe(self.account_authorized.set)
            self.sendProtoOAReconcileReq()
        elif message.payloadType == ProtoOAReconcileRes().payloadType:
            protoOAReconcileRes = Protobuf.extract(message)
            self.open_positions = protoOAReconcileRes.position if hasattr(protoOAReconcileRes, 'position') else []
            self.open_orders = protoOAReconcileRes.order if hasattr(protoOAReconcileRes, 'order') else []
            logging.info("Open Positions and Orders reconciled")
            log_to_sqlite('INFO', "Open Positions and Orders reconciled")
            log_to_bigquery(self.bq_client, 'POSITIONS_RECONCILED', 
                           additional_data={'positions_count': len(self.open_positions), 'orders_count': len(self.open_orders)})
        elif message.payloadType == ProtoOACancelOrderReq().payloadType:
            logging.info("Order has been canceled successfully.")
            log_to_sqlite('INFO', "Order has been canceled successfully.")
            log_to_bigquery(self.bq_client, 'ORDER_CANCELED')
        elif message.payloadType == ProtoOAErrorRes().payloadType:
            protoOAErrorRes = Protobuf.extract(message)
            logging.error(f"Error received: {protoOAErrorRes.errorCode} - {protoOAErrorRes.description}")
            log_to_sqlite('ERROR', f"Error received: {protoOAErrorRes.errorCode} - {protoOAErrorRes.description}")
            log_to_bigquery(self.bq_client, 'API_ERROR', 
                           error_message=f"{protoOAErrorRes.errorCode} - {protoOAErrorRes.description}")
        else:
            logging.info(f"Message received: {Protobuf.extract(message)}")
            log_to_sqlite('INFO', f"Message received: {Protobuf.extract(message)}")

    def onError(self, failure):
        logging.error(f"Message Error: {failure}")
        log_to_sqlite('ERROR', f"Message Error: {failure}")
        log_to_bigquery(self.bq_client, 'MESSAGE_ERROR', error_message=str(failure))
        self.connect_with_retry()

    def sendProtoOAAccountAuthReq(self):
        request = ProtoOAAccountAuthReq()
        request.ctidTraderAccountId = ACCOUNT_ID
        request.accessToken = ACCESS_TOKEN
        deferred = send_request_with_timeout(self.client, request)
        deferred.addErrback(self.onError)

    def sendProtoOAReconcileReq(self):
        request = ProtoOAReconcileReq()
        request.ctidTraderAccountId = ACCOUNT_ID
        deferred = send_request_with_timeout(self.client, request)
        deferred.addErrback(self.onError)

    async def sendProtoOANewOrderReq(self, symbolId, orderType, tradeSide, volume, price=None, label=None, stop_loss_price=None, take_profit_price=None, symbol_precision=None):
        async def send_order():
            request = ProtoOANewOrderReq()
            request.ctidTraderAccountId = ACCOUNT_ID
            request.symbolId = int(symbolId)
            request.orderType = ProtoOAOrderType.Value(orderType.upper())
            request.tradeSide = ProtoOATradeSide.Value(tradeSide.upper())
            request.volume = int(volume * 100)
            request.label = label

            if request.orderType == ProtoOAOrderType.LIMIT and price is not None:
                request.limitPrice = round(float(price), symbol_precision)
            elif request.orderType == ProtoOAOrderType.STOP and price is not None:
                request.stopPrice = round(float(price), symbol_precision)
            
            if stop_loss_price is not None:
                request.stopLoss = round(float(stop_loss_price), symbol_precision)
            
            if take_profit_price is not None:
                request.takeProfit = round(float(take_profit_price), symbol_precision)

            # Log order request to BigQuery
            log_to_bigquery(self.bq_client, 'ORDER_REQUEST_SENT', label, tradeSide, 
                           volume=volume, entry_price=price, stop_loss_price=stop_loss_price, 
                           take_profit_price=take_profit_price)

            future = asyncio.Future()
            
            deferred = send_request_with_timeout(self.client, request)
            deferred.addCallback(lambda result: self.loop.call_soon_threadsafe(future.set_result, result))
            deferred.addErrback(lambda error: self.loop.call_soon_threadsafe(future.set_exception, error))
            
            try:
                response = await asyncio.wait_for(future, timeout=DEFERRED_TIMEOUT + 5)
                if isinstance(response, ProtoMessage):
                    execution_event = Protobuf.extract(response)
                    if isinstance(execution_event, ProtoOAExecutionEvent):
                        logging.info(f"Received execution event for {label}: Type={execution_event.executionType}")
                        log_to_sqlite('INFO', f"Received execution event for {label}: Type={execution_event.executionType}")
                        log_to_bigquery(self.bq_client, 'EXECUTION_EVENT', label, tradeSide, 
                                       execution_type=str(execution_event.executionType))
                        if execution_event.executionType == ProtoOAExecutionType.ORDER_ACCEPTED:
                            logging.info(f"Order has been accepted successfully for {label}")
                            log_to_sqlite('INFO', f"Order has been accepted successfully for {label}")
                            log_to_bigquery(self.bq_client, 'ORDER_ACCEPTED', label, tradeSide, status='accepted')
                            return True
                        else:
                            logging.warning(f"Unexpected execution type for {label}: {execution_event.executionType}")
                            log_to_sqlite('WARNING', f"Unexpected execution type for {label}: {execution_event.executionType}")
                            log_to_bigquery(self.bq_client, 'UNEXPECTED_EXECUTION_TYPE', label, tradeSide, 
                                           execution_type=str(execution_event.executionType))
                            return False
                    else:
                        logging.error(f"Unexpected execution event type for {label}: {type(execution_event)}")
                        log_to_sqlite('ERROR', f"Unexpected execution event type for {label}: {type(execution_event)}")
                        log_to_bigquery(self.bq_client, 'UNEXPECTED_EVENT_TYPE', label, tradeSide, 
                                       error_message=f"Unexpected execution event type: {type(execution_event)}")
                        return False
                else:
                    logging.error(f"Unexpected response type for {label}: {type(response)}")
                    log_to_sqlite('ERROR', f"Unexpected response type for {label}: {type(response)}")
                    log_to_bigquery(self.bq_client, 'UNEXPECTED_RESPONSE_TYPE', label, tradeSide, 
                                   error_message=f"Unexpected response type: {type(response)}")
                    return False
            except Exception as e:
                logging.error(f"Error placing order for {label}: {str(e)}")
                log_to_sqlite('ERROR', f"Error placing order for {label}: {str(e)}")
                log_to_bigquery(self.bq_client, 'ORDER_PLACEMENT_ERROR', label, tradeSide, error_message=str(e))
                raise

        return await self.retry_with_backoff(send_order)
    
    async def cancel_order(self, order_id):
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                request = ProtoOACancelOrderReq()
                request.ctidTraderAccountId = ACCOUNT_ID
                request.orderId = order_id
                future = asyncio.Future()
                deferred = send_request_with_timeout(self.client, request)
                deferred.addCallback(lambda result: self.loop.call_soon_threadsafe(future.set_result, result))
                deferred.addErrback(lambda error: self.loop.call_soon_threadsafe(future.set_exception, error))
                await asyncio.wait_for(future, timeout=DEFERRED_TIMEOUT + 5)
                logging.info(f"Order {order_id} canceled successfully")
                log_to_sqlite('INFO', f"Order {order_id} canceled successfully")
                log_to_bigquery(self.bq_client, 'ORDER_CANCEL_SUCCESS', order_id=str(order_id), status='canceled')
                return True
            except Exception as e:
                logging.error(f"Error canceling order {order_id} (attempt {attempt + 1}): {e}")
                log_to_sqlite('ERROR', f"Error canceling order {order_id} (attempt {attempt + 1}): {e}")
                log_to_bigquery(self.bq_client, 'ORDER_CANCEL_ERROR', order_id=str(order_id), 
                               error_message=str(e), additional_data={'attempt': attempt + 1})
                if attempt < max_attempts - 1:
                    await asyncio.sleep(1)
        return False

    async def close_position(self, position_id, volume):
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                request = ProtoOAClosePositionReq()
                request.ctidTraderAccountId = ACCOUNT_ID
                request.positionId = position_id
                request.volume = volume
                future = asyncio.Future()
                deferred = send_request_with_timeout(self.client, request)
                deferred.addCallback(lambda result: self.loop.call_soon_threadsafe(future.set_result, result))
                deferred.addErrback(lambda error: self.loop.call_soon_threadsafe(future.set_exception, error))
                await asyncio.wait_for(future, timeout=DEFERRED_TIMEOUT + 5)
                logging.info(f"Position {position_id} closed successfully")
                log_to_sqlite('INFO', f"Position {position_id} closed successfully")
                log_to_bigquery(self.bq_client, 'POSITION_CLOSE_SUCCESS', position_id=str(position_id), 
                               volume=volume, status='closed')
                return True
            except Exception as e:
                logging.error(f"Error closing position {position_id} (attempt {attempt + 1}): {e}")
                log_to_sqlite('ERROR', f"Error closing position {position_id} (attempt {attempt + 1}): {e}")
                log_to_bigquery(self.bq_client, 'POSITION_CLOSE_ERROR', position_id=str(position_id), 
                               volume=volume, error_message=str(e), additional_data={'attempt': attempt + 1})
                if attempt < max_attempts - 1:
                    wait_time = (attempt + 1) * 2
                    await asyncio.sleep(wait_time)
        return False

    async def cancel_orders_and_positions(self, label):
        attempt = 0
        while True:
            try:
                await self.refresh_positions_and_orders()

                positions_to_close = [pos for pos in self.open_positions if pos.tradeData.label == label]
                orders_to_cancel = [ord for ord in self.open_orders if ord.tradeData.label == label]

                log_to_bigquery(self.bq_client, 'CANCEL_ORDERS_POSITIONS_START', label, 
                               additional_data={'positions_to_close': len(positions_to_close), 
                                              'orders_to_cancel': len(orders_to_cancel)})

                position_results = await asyncio.gather(*[self.close_position(pos.positionId, pos.tradeData.volume) for pos in positions_to_close], return_exceptions=True)
                order_results = await asyncio.gather(*[self.cancel_order(ord.orderId) for ord in orders_to_cancel], return_exceptions=True)

                await asyncio.sleep(2)
                await self.refresh_positions_and_orders()

                remaining_positions = [pos for pos in self.open_positions if pos.tradeData.label == label]
                remaining_orders = [ord for ord in self.open_orders if ord.tradeData.label == label]

                if not remaining_positions and not remaining_orders:
                    logging.info(f"All positions and orders for label {label} closed/canceled successfully")
                    log_to_sqlite('INFO', f"All positions and orders for label {label} closed/canceled successfully")
                    log_to_bigquery(self.bq_client, 'CANCEL_ORDERS_POSITIONS_SUCCESS', label, status='completed')
                    return True

                attempt += 1
                logging.warning(f"Attempt {attempt} to close all positions and cancel all orders for {label} failed. Retrying...")
                log_to_sqlite('WARNING', f"Attempt {attempt} to close all positions and cancel all orders for {label} failed. Retrying...")
                log_to_bigquery(self.bq_client, 'CANCEL_ORDERS_POSITIONS_RETRY', label, 
                               additional_data={'attempt': attempt})
                await asyncio.sleep(2)

            except asyncio.TimeoutError:
                logging.warning(f"Timeout refreshing positions and orders (attempt {attempt + 1})")
                log_to_sqlite('WARNING', f"Timeout refreshing positions and orders (attempt {attempt + 1})")
                log_to_bigquery(self.bq_client, 'REFRESH_TIMEOUT', label, error_message="Timeout refreshing positions and orders")
                await asyncio.sleep(5)
            except Exception as e:
                logging.error(f"Error in cancel_orders_and_positions: {e}")
                log_to_sqlite('ERROR', f"Error in cancel_orders_and_positions: {e}")
                log_to_bigquery(self.bq_client, 'CANCEL_ORDERS_POSITIONS_ERROR', label, error_message=str(e))
                await asyncio.sleep(5)

    async def refresh_positions_and_orders(self):
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                request = ProtoOAReconcileReq()
                request.ctidTraderAccountId = ACCOUNT_ID
                future = asyncio.Future()
                deferred = send_request_with_timeout(self.client, request)
                deferred.addCallback(lambda result: self.loop.call_soon_threadsafe(future.set_result, result))
                deferred.addErrback(lambda error: self.loop.call_soon_threadsafe(future.set_exception, error))
                response = await asyncio.wait_for(future, timeout=DEFERRED_TIMEOUT + 5)
                protoOAReconcileRes = Protobuf.extract(response)
                self.open_positions = protoOAReconcileRes.position if hasattr(protoOAReconcileRes, 'position') else []
                self.open_orders = protoOAReconcileRes.order if hasattr(protoOAReconcileRes, 'order') else []
                logging.info("Positions and Orders refreshed")
                log_to_sqlite('INFO', "Positions and Orders refreshed")
                log_to_bigquery(self.bq_client, 'REFRESH_SUCCESS', 
                               additional_data={'positions_count': len(self.open_positions), 'orders_count': len(self.open_orders)})
                return
            except asyncio.TimeoutError:
                logging.error(f"Timeout while refreshing positions and orders (attempt {attempt + 1})")
                log_to_sqlite('ERROR', f"Timeout while refreshing positions and orders (attempt {attempt + 1})")
                log_to_bigquery(self.bq_client, 'REFRESH_TIMEOUT_ERROR', additional_data={'attempt': attempt + 1})
            except Exception as e:
                logging.error(f"Error refreshing positions and orders (attempt {attempt + 1}): {e}")
                log_to_sqlite('ERROR', f"Error refreshing positions and orders (attempt {attempt + 1}): {e}")
                log_to_bigquery(self.bq_client, 'REFRESH_ERROR', error_message=str(e), additional_data={'attempt': attempt + 1})
            
            if attempt < max_attempts - 1:
                wait_time = (attempt + 1) * 2
                await asyncio.sleep(wait_time)
        
        raise Exception("Failed to refresh positions and orders after multiple attempts")
        
    def get_recent_signal(self, signal_file):
        try:
            spec = importlib.util.spec_from_file_location("signal_module", signal_file)
            signal_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(signal_module)
            return signal_module.Action
        except Exception as e:
            logging.error(f"Error reading signal from {signal_file}: {e}")
            log_to_sqlite('ERROR', f"Error reading signal from {signal_file}: {e}")
            log_to_bigquery(self.bq_client, 'SIGNAL_READ_ERROR', error_message=str(e), 
                           additional_data={'signal_file': signal_file})
            return None

    def check_new_signal(self, strategy):
        try:
            if not self.are_quotes_live(strategy):
                return False

            signal = self.get_recent_signal(strategy['signal_file'])
            if signal != strategy.get('last_signal'):
                self.handle_new_signal(strategy, signal)
            return True
        except Exception as e:
            logging.error(f"Error checking new signal for {strategy['label']}: {e}")
            log_to_sqlite('ERROR', f"Error checking new signal for {strategy['label']}: {e}")
            log_to_bigquery(self.bq_client, 'SIGNAL_CHECK_ERROR', strategy['label'], error_message=str(e))
            return False

    def are_quotes_live(self, strategy):
        try:
            spec = importlib.util.spec_from_file_location("quotes_module", strategy['depth_quotes_file'])
            quotes_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(quotes_module)
            
            timestamp = quotes_module.timestamp
            timestamp_datetime = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')
            current_datetime = datetime.datetime.now()
            time_diff = current_datetime - timestamp_datetime

            quotes_live = time_diff <= datetime.timedelta(seconds=10)
            
            if not quotes_live:
                self.handle_stale_quotes(strategy, current_datetime)
                log_to_bigquery(self.bq_client, 'QUOTES_STALE', strategy['label'], quotes_live=False,
                               additional_data={'time_diff_seconds': time_diff.total_seconds()})
                return False
            
            log_to_bigquery(self.bq_client, 'QUOTES_LIVE_CHECK', strategy['label'], quotes_live=True,
                           best_bid=getattr(quotes_module, 'bid', None), 
                           best_ask=getattr(quotes_module, 'ask', None))
            return True
        except Exception as e:
            logging.error(f"Error checking quote liveness for {strategy['label']}: {e}")
            log_to_sqlite('ERROR', f"Error checking quote liveness for {strategy['label']}: {e}")
            log_to_bigquery(self.bq_client, 'QUOTES_CHECK_ERROR', strategy['label'], error_message=str(e))
            return False

    def handle_stale_quotes(self, strategy, current_datetime):
        warning_message = f"{current_datetime.strftime('%m/%d/%Y %H:%M:%S')}, Strategy: \"{strategy['label']}\", WARNING: Quotes not live. Waiting for market to come back online."
        print(warning_message)
        log_to_sqlite('WARNING', warning_message)
        log_to_bigquery(self.bq_client, 'QUOTES_STALE_WARNING', strategy['label'], 
                       error_message="Quotes not live. Waiting for market to come back online.")
        self.loop.call_later(10, lambda: self.check_new_signal(strategy))

    def handle_new_signal(self, strategy, signal):
        print(f"New signal detected for {strategy['label']}: {signal}")
        log_to_sqlite('INFO', f"New signal detected for {strategy['label']}: {signal}")
        log_to_bigquery(self.bq_client, 'NEW_SIGNAL_HANDLED', strategy['label'], signal)
        strategy['last_signal'] = signal
        if self.account_authorized.is_set():
            self.sendProtoOAReconcileReq()
        else:
            print("Account is not yet authorized. Adding signal to queue.")
            log_to_sqlite('INFO', "Account is not yet authorized. Adding signal to queue.")
            log_to_bigquery(self.bq_client, 'SIGNAL_QUEUED', strategy['label'], signal, 
                           status='queued_not_authorized')
            self.signal_queue.put((strategy, signal))

    async def process_signal(self, signal, strategy):
        try:
            logging.info(f"BEGIN: Processing signal for {strategy['label']}: {signal}")
            log_to_sqlite('INFO', f"BEGIN: Processing signal for {strategy['label']}: {signal}")
            log_to_bigquery(self.bq_client, 'SIGNAL_PROCESSING_START', strategy['label'], signal)

            quotes_fresh = self.are_quotes_live(strategy)
            logging.info(f"Quotes fresh for {strategy['label']}: {quotes_fresh}")
            log_to_sqlite('INFO', f"Quotes fresh for {strategy['label']}: {quotes_fresh}")

            entry_price = stop_loss_price = take_profit_price = None
            best_bid = best_ask = None

            if quotes_fresh:
                spec = importlib.util.spec_from_file_location("quotes_module", strategy['depth_quotes_file'])
                quotes_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(quotes_module)
                best_bid = quotes_module.bid
                best_ask = quotes_module.ask
                logging.info(f"Quotes for {strategy['label']}: Bid: {best_bid}, Ask: {best_ask}")
                log_to_sqlite('INFO', f"Quotes for {strategy['label']}: Bid: {best_bid}, Ask: {best_ask}")

                if signal.lower() == "buy":
                    if strategy['buy_offset_percentage'] < 0:
                        entry_price = round(best_ask + abs(strategy['buy_offset_percentage']) / 100, strategy['symbol_precision'])
                    else:
                        entry_price = round(best_bid * (1 - strategy['buy_offset_percentage'] / 100), strategy['symbol_precision'])

                    stop_loss_price = round(entry_price * (1 - strategy['stop_loss_percentage'] / 100), strategy['symbol_precision'])
                    
                    if strategy['take_profit_percentage'] != 0:
                        take_profit_price = round(entry_price * (1 + strategy['take_profit_percentage'] / 100), strategy['symbol_precision'])
                
                elif signal.lower() == "sell":
                    if strategy['sell_offset_percentage'] < 0:
                        entry_price = round(best_bid - abs(strategy['sell_offset_percentage']) / 100, strategy['symbol_precision'])
                    else:
                        entry_price = round(best_ask * (1 + strategy['sell_offset_percentage'] / 100), strategy['symbol_precision'])

                    stop_loss_price = round(entry_price * (1 + strategy['stop_loss_percentage'] / 100), strategy['symbol_precision'])
                    
                    if strategy['take_profit_percentage'] != 0:
                        take_profit_price = round(entry_price * (1 - strategy['take_profit_percentage'] / 100), strategy['symbol_precision'])
                
                else:
                    logging.warning(f"Unknown signal for {strategy['label']}: {signal}")
                    log_to_sqlite('WARNING', f"Unknown signal for {strategy['label']}: {signal}")
                    log_to_bigquery(self.bq_client, 'UNKNOWN_SIGNAL', strategy['label'], signal, 
                                   error_message=f"Unknown signal: {signal}")
                    return False

                logging.info(f"Prices calculated for {strategy['label']}: Entry: {entry_price}, SL: {stop_loss_price}, TP: {take_profit_price}")
                log_to_sqlite('INFO', f"Prices calculated for {strategy['label']}: Entry: {entry_price}, SL: {stop_loss_price}, TP: {take_profit_price}")

            else:
                logging.warning(f"Quotes not fresh for {strategy['label']}. Unable to calculate prices immediately.")
                log_to_sqlite('WARNING', f"Quotes not fresh for {strategy['label']}. Unable to calculate prices immediately.")

            order_data = {
                'signal': signal,
                'entry_price': entry_price,
                'stop_loss_price': stop_loss_price,
                'take_profit_price': take_profit_price,
                'timestamp': datetime.datetime.now().isoformat(),
                'prices_calculated': quotes_fresh,
                'symbol_id': strategy['symbol_id'],
                'volume': strategy['volume'],
                'symbol_precision': strategy['symbol_precision']
            }
            
            log_to_bigquery(self.bq_client, 'PRICES_CALCULATED', strategy['label'], signal,
                           volume=strategy['volume'], entry_price=entry_price, 
                           stop_loss_price=stop_loss_price, take_profit_price=take_profit_price,
                           best_bid=best_bid, best_ask=best_ask, quotes_live=quotes_fresh)
            
            self.order_manager.add_order(strategy['label'], order_data)
            logging.info(f"Added pending order for {strategy['label']}: {order_data}")
            log_to_sqlite('INFO', f"Added pending order for {strategy['label']}: {order_data}")
            
            # Don't process immediately - let continuous_pending_order_check handle it
            logging.info(f"Order added to queue for {strategy['label']}, will be processed by continuous order check")
            log_to_sqlite('INFO', f"Order added to queue for {strategy['label']}, will be processed by continuous order check")

            return True

        except Exception as e:
            logging.error(f"Error processing signal for {strategy['label']}: {e}")
            log_to_sqlite('ERROR', f"Error processing signal for {strategy['label']}: {e}")
            log_to_bigquery(self.bq_client, 'SIGNAL_PROCESSING_ERROR', strategy['label'], signal, error_message=str(e))
            return False
        finally:
            logging.info(f"END: Processing signal for {strategy['label']}")
            log_to_sqlite('INFO', f"END: Processing signal for {strategy['label']}")
            log_to_bigquery(self.bq_client, 'SIGNAL_PROCESSING_END', strategy['label'], signal)

    async def process_specific_pending_order(self, label, order):
        try:
            logging.info(f"BEGIN: Processing specific pending order for {label}")
            log_to_sqlite('INFO', f"BEGIN: Processing specific pending order for {label}")
            log_to_bigquery(self.bq_client, 'SPECIFIC_ORDER_PROCESSING_START', label, order.get('signal'))

            if not self.account_authorized.is_set():
                logging.warning(f"Account not authorized. Skipping pending order for {label}")
                log_to_sqlite('WARNING', f"Account not authorized. Skipping pending order for {label}")
                log_to_bigquery(self.bq_client, 'ACCOUNT_NOT_AUTHORIZED', label, order.get('signal'), 
                               status='skipped_not_authorized')
                return

            if not order.get('prices_calculated', False):
                logging.info(f"Prices not yet calculated for {label}, attempting to calculate now")
                log_to_sqlite('INFO', f"Prices not yet calculated for {label}, attempting to calculate now")
                strategy = next((s for s in self.strategies if s['label'] == label), None)
                if strategy and self.are_quotes_live(strategy):
                    await self.calculate_prices_for_pending_order(strategy, order)
                else:
                    logging.info(f"Unable to calculate prices for {label}, skipping for now")
                    log_to_sqlite('INFO', f"Unable to calculate prices for {label}, skipping for now")
                    log_to_bigquery(self.bq_client, 'PRICES_NOT_CALCULATED', label, order.get('signal'), 
                                   status='skipped_no_prices')
                    return

            await self.cancel_orders_and_positions(label)

            self.order_manager.mark_order_processing(label)

            order_placed = await self.sendProtoOANewOrderReq(
                order['symbol_id'], 
                "LIMIT", 
                order['signal'].upper(), 
                order['volume'], 
                order['entry_price'], 
                label, 
                order['stop_loss_price'], 
                order['take_profit_price'],
                order['symbol_precision']
            )

            if order_placed:
                self.order_manager.remove_order(label)
                logging.info(f"Processed pending order for {label}")
                log_to_sqlite('INFO', f"Processed pending order for {label}")
                log_to_bigquery(self.bq_client, 'ORDER_PROCESSED_SUCCESS', label, order.get('signal'), status='processed')
            else:
                self.order_manager.add_order(label, order)
                logging.warning(f"Failed to process pending order for {label}, will retry later")
                log_to_sqlite('WARNING', f"Failed to process pending order for {label}, will retry later")
                log_to_bigquery(self.bq_client, 'ORDER_PROCESSED_FAILED', label, order.get('signal'), 
                               status='failed_will_retry')

        except Exception as e:
            logging.error(f"Error processing specific pending order for {label}: {e}")
            log_to_sqlite('ERROR', f"Error processing specific pending order for {label}: {e}")
            log_to_bigquery(self.bq_client, 'SPECIFIC_ORDER_PROCESSING_ERROR', label, order.get('signal'), 
                           error_message=str(e))
        finally:
            logging.info(f"END: Processing specific pending order for {label}")
            log_to_sqlite('INFO', f"END: Processing specific pending order for {label}")
            log_to_bigquery(self.bq_client, 'SPECIFIC_ORDER_PROCESSING_END', label, order.get('signal'))

    async def check_fresh_quotes_for_pending_orders(self):
        while True:
            try:
                updated_orders = False
                for label, order in self.order_manager.get_pending_orders().items():
                    if not order.get('prices_calculated', False):
                        strategy = next((s for s in self.strategies if s['label'] == label), None)
                        if strategy and self.are_quotes_live(strategy):
                            await self.calculate_prices_for_pending_order(strategy, order)
                            updated_orders = True
                if updated_orders:
                    self.fresh_prices_event.set()
                await asyncio.sleep(1)
            except Exception as e:
                logging.error(f"Error in check_fresh_quotes_for_pending_orders: {e}")
                log_to_sqlite('ERROR', f"Error in check_fresh_quotes_for_pending_orders: {e}")
                log_to_bigquery(self.bq_client, 'FRESH_QUOTES_CHECK_ERROR', error_message=str(e))
                await asyncio.sleep(5)

    async def wait_and_process_fresh_prices(self):
        while True:
            try:
                await self.fresh_prices_event.wait()
                await self.process_pending_orders()
                self.fresh_prices_event.clear()
            except Exception as e:
                logging.error(f"Error in wait_and_process_fresh_prices: {e}")
                log_to_sqlite('ERROR', f"Error in wait_and_process_fresh_prices: {e}")
                log_to_bigquery(self.bq_client, 'FRESH_PRICES_PROCESSING_ERROR', error_message=str(e))
                await asyncio.sleep(5)
        
    async def calculate_prices_for_pending_order(self, strategy, order):
        try:
            spec = importlib.util.spec_from_file_location("quotes_module", strategy['depth_quotes_file'])
            quotes_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(quotes_module)
            best_bid = quotes_module.bid
            best_ask = quotes_module.ask

            signal = order['signal']
            if signal.lower() == "buy":
                if strategy['buy_offset_percentage'] < 0:
                    entry_price = round(best_ask + abs(strategy['buy_offset_percentage']) / 100, strategy['symbol_precision'])
                else:
                    entry_price = round(best_bid * (1 - strategy['buy_offset_percentage'] / 100), strategy['symbol_precision'])

                stop_loss_price = round(entry_price * (1 - strategy['stop_loss_percentage'] / 100), strategy['symbol_precision'])
                
                if strategy['take_profit_percentage'] != 0:
                    take_profit_price = round(entry_price * (1 + strategy['take_profit_percentage'] / 100), strategy['symbol_precision'])
                else:
                    take_profit_price = None
            
            elif signal.lower() == "sell":
                if strategy['sell_offset_percentage'] < 0:
                    entry_price = round(best_bid - abs(strategy['sell_offset_percentage']) / 100, strategy['symbol_precision'])
                else:
                    entry_price = round(best_ask * (1 + strategy['sell_offset_percentage'] / 100), strategy['symbol_precision'])

                stop_loss_price = round(entry_price * (1 + strategy['stop_loss_percentage'] / 100), strategy['symbol_precision'])
                
                if strategy['take_profit_percentage'] != 0:
                    take_profit_price = round(entry_price * (1 - strategy['take_profit_percentage'] / 100), strategy['symbol_precision'])
                else:
                    take_profit_price = None

            order['entry_price'] = entry_price
            order['stop_loss_price'] = stop_loss_price
            order['take_profit_price'] = take_profit_price
            order['prices_calculated'] = True
            self.order_manager.add_order(strategy['label'], order)

            logging.info(f"Prices calculated for pending order {strategy['label']}: Entry: {entry_price}, SL: {stop_loss_price}, TP: {take_profit_price}")
            log_to_sqlite('INFO', f"Prices calculated for pending order {strategy['label']}: Entry: {entry_price}, SL: {stop_loss_price}, TP: {take_profit_price}")
            log_to_bigquery(self.bq_client, 'PENDING_ORDER_PRICES_CALCULATED', strategy['label'], signal,
                           volume=order['volume'], entry_price=entry_price, 
                           stop_loss_price=stop_loss_price, take_profit_price=take_profit_price,
                           best_bid=best_bid, best_ask=best_ask, quotes_live=True)

        except Exception as e:
            logging.error(f"Error calculating prices for pending order {strategy['label']}: {e}")
            log_to_sqlite('ERROR', f"Error calculating prices for pending order {strategy['label']}: {e}")
            log_to_bigquery(self.bq_client, 'PENDING_ORDER_PRICE_CALCULATION_ERROR', strategy['label'], 
                           order.get('signal'), error_message=str(e))

def main():
    set_terminal_title("EMS-BENCHMARK")
    max_restarts = 5
    restart_count = 0

    while restart_count < max_restarts:
        try:
            trading_system = TradingSystem(strategies)
            log_to_bigquery(trading_system.bq_client, 'SYSTEM_START', 
                           additional_data={'restart_count': restart_count, 'max_restarts': max_restarts})
            
            reactor_thread = threading.Thread(target=reactor.run, args=(False,))
            reactor_thread.start()
            
            trading_system.start()
        except KeyboardInterrupt:
            logging.info("Shutting down...")
            log_to_sqlite('INFO', "Shutting down...")
            if 'trading_system' in locals() and trading_system.bq_client:
                log_to_bigquery(trading_system.bq_client, 'SYSTEM_SHUTDOWN', status='manual_shutdown')
            break
        except Exception as e:
            logging.error(f"Unexpected error in main loop: {e}")
            log_to_sqlite('ERROR', f"Unexpected error in main loop: {e}")
            if 'trading_system' in locals() and trading_system.bq_client:
                log_to_bigquery(trading_system.bq_client, 'SYSTEM_ERROR', error_message=str(e))
            restart_count += 1
            logging.info(f"Attempting to restart (attempt {restart_count}/{max_restarts})...")
            log_to_sqlite('INFO', f"Attempting to restart (attempt {restart_count}/{max_restarts})...")
            if 'trading_system' in locals() and trading_system.bq_client:
                log_to_bigquery(trading_system.bq_client, 'SYSTEM_RESTART_ATTEMPT', 
                               additional_data={'restart_count': restart_count, 'max_restarts': max_restarts})
            time.sleep(10)
        finally:
            if 'trading_system' in locals():
                for observer in trading_system.observers:
                    observer.stop()
                for observer in trading_system.observers:
                    observer.join()
            if 'reactor_thread' in locals():
                reactor.callFromThread(reactor.stop)
                reactor_thread.join()
            if 'trading_system' in locals() and trading_system.loop:
                trading_system.loop.close()

    if restart_count == max_restarts:
        error_message = "Max restart attempts reached. Please check the logs and restart the script manually."
        logging.error(error_message)
        log_to_sqlite('ERROR', error_message)
        if 'trading_system' in locals() and trading_system.bq_client:
            log_to_bigquery(trading_system.bq_client, 'SYSTEM_MAX_RESTARTS_REACHED', error_message=error_message)

if __name__ == "__main__":
    main()
