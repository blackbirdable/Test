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
from StrategyConfigAC2 import strategies, CLIENT_ID, CLIENT_SECRET, ACCESS_TOKEN, ACCOUNT_ID
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
import numpy as np

# Global variables
STOP_LOSS_PERCENTAGE = 1.5    # 1.5% stop loss from entry price of WeightedSharePrice (was take profit)
TAKE_PROFIT_PERCENTAGE = 1.0  # 1.0% take profit from entry price of WeightedSharePrice (was stop loss)
RIP_THRESHOLD_PERCENTAGE = 2.0  # 2% rip to trigger mirroring
CANDLE_PERIOD_MINUTES = 25    # OHLC candle period in minutes

def set_terminal_title(title):
    ctypes.windll.kernel32.SetConsoleTitleW(title)

def log_to_sqlite(event_type, message):
    db_path = r'C:\Users\Administrator\Desktop\EMS_Log_STR_AC2.db'
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
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute('''
                INSERT INTO logs (timestamp, event_type, message)
                VALUES (?, ?, ?)
            ''', (timestamp, event_type, message))
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

class SignalHandler(FileSystemEventHandler):
    def __init__(self, strategy, signal_queue):
        self.strategy = strategy
        self.signal_queue = signal_queue

    def on_modified(self, event):
        if event.src_path.endswith(os.path.basename(self.strategy['signal_file'])):
            signal = self.get_recent_signal(self.strategy['signal_file'])
            if signal is None:
                logging.warning(f"Failed to get a valid signal from {self.strategy['signal_file']}. Skipping this update.")
                log_to_sqlite('WARNING', f"Failed to get a valid signal from {self.strategy['signal_file']}. Skipping this update.")
                return
            if signal != self.strategy.get('last_signal'):
                logging.info(f"New signal detected for {self.strategy['label']}: {signal}")
                log_to_sqlite('INFO', f"New signal detected for {self.strategy['label']}: {signal}")
                self.strategy['last_signal'] = signal
                self.signal_queue.put((self.strategy, signal))

    def get_recent_signal(self, signal_file):
        try:
            spec = importlib.util.spec_from_file_location("signal_module", signal_file)
            signal_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(signal_module)
            original_signal = signal_module.Action
            
            # For sell-the-rip strategy: keep original signal as is
            # When price rips up, original signal should be "sell" to sell the rip
            # When price dips down, original signal should be "buy" but we don't want to buy the dip
            inverted_signal = original_signal
                
            return inverted_signal
        except Exception as e:
            logging.error(f"Error reading signal from {signal_file}: {e}")
            log_to_sqlite('ERROR', f"Error reading signal from {signal_file}: {e}")
            return None

class OrderManager:
    def __init__(self, file_path='pending_orders_AC2.json'):
        self.file_path = file_path
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
        else:
            logging.warning(f"Order {label} not found for marking as processing")
            log_to_sqlite('WARNING', f"Order {label} not found for marking as processing")

    def remove_order(self, label):
        data = self._read_file()
        if label in data:
            del data[label]  # Remove the order regardless of its status
            self._write_file(data)
            logging.info(f"Removed order {label}")
            log_to_sqlite('INFO', f"Removed order {label}")
        else:
            logging.info(f"Order {label} not found for removal, already processed")
            log_to_sqlite('INFO', f"Order {label} not found for removal, already processed")

    def add_order(self, label, order_data):
        data = self._read_file()
        order_data['status'] = 'pending'
        data[label] = order_data
        self._write_file(data)
        logging.info(f"Added new pending order {label} to {self.file_path}")
        log_to_sqlite('INFO', f"Added new pending order {label} to {self.file_path}")

    def get_pending_orders(self):
        data = self._read_file()
        return {k: v for k, v in data.items() if v['status'] == 'pending'}

    def get_processing_orders(self):
        data = self._read_file()
        return {k: v for k, v in data.items() if v['status'] == 'processing'}

# Define SocketManager class before it's used
class SocketManager:
    def __init__(self, ports):
        self.ports = ports
        self.sockets = {}
        self.paused = False

    def create_socket(self, port):
        """Create and bind a socket to a specific port."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('0.0.0.0', port))  # Bind to all available interfaces
            s.listen(5)
            logging.info(f"Socket created and listening on port {port}")
            return s
        except socket.error as e:
            logging.error(f"Error creating socket on port {port}: {e}")
            return None

    def close_socket(self, s, port):
        """Close and cleanup the socket."""
        try:
            s.close()
            logging.info(f"Socket on port {port} closed and cleaned up.")
        except Exception as e:
            logging.error(f"Error closing socket on port {port}: {e}")

    def restart_sockets(self):
        """Restart sockets on specified ports."""
        logging.info("Restarting sockets...")
        self.pause_order_processing()  # Pause order processing
        self.cleanup_sockets()         # Clean up sockets
        self.create_sockets()          # Recreate the sockets
        self.resume_order_processing() # Resume order processing
        logging.info("Sockets restarted.")

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

    def resume_order_processing(self):
        """Resume order processing."""
        self.paused = False
        logging.info("Order processing resumed after socket restart.")

class OHLCCalculator:
    def __init__(self, db_path=r"C:\Users\Administrator\Desktop\TradingData_STR_AC2.db"):
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
        self.cooldown_until = None  # Timestamp until when rip detection is disabled
        self.rip_already_triggered = False  # Flag to track if a rip has been triggered in this session
        
    def fetch_latest_data(self):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get the latest data from Summary table
            cursor.execute('''
                SELECT Timestamp, WeightedSharePrice
                FROM Summary
                ORDER BY id DESC
                LIMIT 1
            ''')
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                timestamp = datetime.datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S')
                price = result[1]
                return timestamp, price
            
            return None, None
            
        except sqlite3.Error as e:
            logging.error(f"Error fetching latest data: {e}")
            log_to_sqlite('ERROR', f"Error fetching latest data: {e}")
            return None, None
    
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
                log_to_sqlite('INFO', f"Closing candle: Start={self.current_candle['timestamp'].strftime('%H:%M')} O:{self.current_candle['open']:.2f} H:{self.current_candle['high']:.2f} L:{self.current_candle['low']:.2f} C:{self.current_candle['close']:.2f}")
            
            # Start a new candle
            self.current_candle = {
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'timestamp': candle_start_time  # Use the standard candle start time
            }
            
            logging.info(f"Starting new candle at {candle_start_time.strftime('%H:%M')} with opening price {price:.2f}")
            log_to_sqlite('INFO', f"Starting new candle at {candle_start_time.strftime('%H:%M')} with opening price {price:.2f}")
            
            # Check if cooldown period should end with the new candle
            if self.cooldown_until and candle_start_time > self.cooldown_until:
                logging.info("Cooldown period ended with new candle")
                log_to_sqlite('INFO', "Cooldown period ended with new candle")
                self.cooldown_until = None
        else:
            # Update the current candle
            self.current_candle['high'] = max(self.current_candle['high'], price)
            self.current_candle['low'] = min(self.current_candle['low'], price)
            self.current_candle['close'] = price
        
        return self.current_candle
    
    def _should_start_new_candle(self, timestamp):
        """This method is deprecated and replaced by the _get_candle_start_time approach,
        but kept for backward compatibility."""
        if self.current_candle['timestamp'] is None:
            return True
            
        current_candle_start = self._get_candle_start_time(self.current_candle['timestamp'])
        new_candle_start = self._get_candle_start_time(timestamp)
        
        return current_candle_start != new_candle_start
    
    def check_rip_threshold(self):
        """Check if the current candle has a rip greater than the threshold percentage"""
        # Check if we're in a cooldown period
        if self.cooldown_until is not None and datetime.datetime.now() < self.cooldown_until:
            logging.info(f"In cooldown period until {self.cooldown_until.strftime('%H:%M:%S')}, skipping rip check")
            return False
            
        # Check if a rip has already been triggered in this trading session
        if self.rip_already_triggered:
            logging.info("Rip already triggered in this session, no more rip selling allowed")
            log_to_sqlite('INFO', "Rip already triggered in this session, no more rip selling allowed")
            return False
            
        if self.current_candle['open'] is None:
            return False
            
        high = self.current_candle['high']
        low = self.current_candle['low']
        
        if low == 0:  # Avoid division by zero
            return False
            
        rip_percentage = ((high - low) / low) * 100
        
        if rip_percentage >= RIP_THRESHOLD_PERCENTAGE:
            logging.info(f"Rip threshold detected: {rip_percentage:.2f}% (threshold: {RIP_THRESHOLD_PERCENTAGE}%)")
            log_to_sqlite('INFO', f"Rip threshold detected: {rip_percentage:.2f}% (threshold: {RIP_THRESHOLD_PERCENTAGE}%)")
            self.rip_already_triggered = True  # Mark that a rip has been triggered in this session
            return True
        
        return False
    
    def get_current_candle(self):
        return self.current_candle
    
    def get_candles(self):
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
            log_to_sqlite('INFO', f"Cooldown set until start of next candle: {next_candle_start.strftime('%H:%M:%S')}")
        else:
            # If no current candle, set cooldown based on current time
            now = datetime.datetime.now()
            current_candle_start = self._get_candle_start_time(now)
            next_candle_start = current_candle_start + datetime.timedelta(minutes=self.interval_minutes)
            
            self.cooldown_until = next_candle_start
            logging.info(f"No current candle, cooldown set until {next_candle_start.strftime('%H:%M:%S')}")
            log_to_sqlite('INFO', f"No current candle, cooldown set until {next_candle_start.strftime('%H:%M:%S')}")
    
    def reset_for_new_session(self):
        """Reset the rip trigger flag for a new trading session"""
        self.rip_already_triggered = False
        self.cooldown_until = None
        logging.info("OHLC calculator reset for new trading session")
        log_to_sqlite('INFO', "OHLC calculator reset for new trading session")

class PositionMirror:
    def __init__(self, pnl_log_path=r"C:\Users\Administrator\Desktop\Sonixen\Tools\Ctrader OpenAPI\EMS STR_SQL_DB\Pnl_Log.py"):
        self.pnl_log_path = pnl_log_path
        self.mirrored_positions = {}  # Dictionary to track mirrored positions
        self.mirrored_orders = {}  # Dictionary to track mirrored orders
        self.entry_price = None  # WeightedSharePrice at time of mirroring (portfolio-wide)
    
    def mirror_order(self, position):
        """Mirror a single position/order for sell-the-rip strategy."""
        mirrored_position = position.copy()
        
        # Keep the same order type and volume - no inversion needed for sell-the-rip
        volume = position.get('Volume', 0)
        if volume > 0:  # BUY position
            order_type = "BUY"
        else:  # SELL position 
            order_type = "SELL"
        
        # Keep TP and SL levels as they are - no swapping needed
        original_tp = position.get('CurrentTP')
        original_sl = position.get('CurrentSL')
        
        logging.info(f"Mirrored {position.get('Label', 'Unknown')} order: "
                    f"Volume: {volume} ({order_type}), "
                    f"TP: {original_tp}, "
                    f"SL: {original_sl}")
        log_to_sqlite('INFO', f"Mirrored {position.get('Label', 'Unknown')} order: "
                    f"Volume: {volume} ({order_type}), "
                    f"TP: {original_tp}, "
                    f"SL: {original_sl}")
        
        return mirrored_position
    
    def read_pnl_log(self):
        try:
            with open(self.pnl_log_path, 'r') as f:
                content = f.read()
                
            # Extract the JSON part
            json_str = content.replace('pnl_data = ', '')
            data = json.loads(json_str)
            
            return data
        except Exception as e:
            logging.error(f"Error reading Pnl_Log.py: {e}")
            log_to_sqlite('ERROR', f"Error reading Pnl_Log.py: {e}")
            return None
            
    
    def mirror_positions(self, current_price, strategies):
        """Mirror all positions and orders, and set entry price to current WeightedSharePrice.
        Uses volumes from StrategyConfigAC2, other parameters from Pnl_Log.py."""
        data = self.read_pnl_log()
        if not data:
            return False
        
        # Set entry price for portfolio-wide TP/SL
        self.entry_price = current_price
        logging.info(f"Setting entry price for portfolio-wide TP/SL: {self.entry_price:.2f}")
        log_to_sqlite('INFO', f"Setting entry price for portfolio-wide TP/SL: {self.entry_price:.2f}")
        
        positions = data.get('positions', [])
        orders = data.get('open_orders', [])
        
        # Create a lookup dictionary for strategy configs by label
        strategy_configs = {s['label']: s for s in strategies}
        
        # Mirror and invert positions with volumes from StrategyConfigAC2
        for position in positions:
            label = position.get('Label')
            # Update volume from StrategyConfigAC2 if available
            if label in strategy_configs:
                position_copy = position.copy()  # Create a copy to avoid modifying the original
                position_copy['Volume'] = strategy_configs[label]['volume']
                logging.info(f"Using volume from StrategyConfigAC2 for {label}: {position_copy['Volume']}")
                # Mirror the position for sell-the-rip strategy
                mirrored_position = self.mirror_order(position_copy)
                self.mirrored_positions[label] = mirrored_position
            else:
                # If not found in strategy config, mirror as is
                mirrored_position = self.mirror_order(position)
                self.mirrored_positions[label] = mirrored_position
                logging.warning(f"Strategy config not found for {label}, using original volume: {position.get('Volume')}")
            
        # Mirror and invert open orders with volumes from StrategyConfigAC2
        for order in orders:
            label = order.get('Label')
            # Update volume from StrategyConfigAC2 if available
            if label in strategy_configs:
                order_copy = order.copy()  # Create a copy to avoid modifying the original
                order_copy['Volume'] = strategy_configs[label]['volume']
                logging.info(f"Using volume from StrategyConfigAC2 for order {label}: {order_copy['Volume']}")
                # Mirror the order for sell-the-rip strategy
                mirrored_order = self.mirror_order(order_copy)
                self.mirrored_orders[label] = mirrored_order
            else:
                # If not found in strategy config, mirror as is
                mirrored_order = self.mirror_order(order)
                self.mirrored_orders[label] = mirrored_order
                logging.warning(f"Strategy config not found for order {label}, using original volume: {order.get('Volume')}")
        
        logging.info(f"Mirrored {len(positions)} positions and {len(orders)} orders at WeightedSharePrice {current_price:.2f}")
        log_to_sqlite('INFO', f"Mirrored {len(positions)} positions and {len(orders)} orders at WeightedSharePrice {current_price:.2f}")
        
        return True
    
    def check_portfolio_tp_sl(self, current_price):
        """Check if the portfolio-wide take profit or stop loss has been hit"""
        if self.entry_price is None:
            return False
        
        # Calculate percentage change from entry
        percent_change = ((current_price - self.entry_price) / self.entry_price) * 100
        
        # For sell-the-rip strategy: TP when price goes UP (profit from selling high), SL when price goes DOWN (loss from selling low)
        if percent_change >= TAKE_PROFIT_PERCENTAGE:
            logging.info(f"Portfolio take profit hit: Current price {current_price:.2f} is {percent_change:.2f}% above entry price {self.entry_price:.2f}")
            log_to_sqlite('INFO', f"Portfolio take profit hit: Current price {current_price:.2f} is {percent_change:.2f}% above entry price {self.entry_price:.2f}")
            return True
        elif percent_change <= -STOP_LOSS_PERCENTAGE:
            logging.info(f"Portfolio stop loss hit: Current price {current_price:.2f} is {abs(percent_change):.2f}% below entry price {self.entry_price:.2f}")
            log_to_sqlite('INFO', f"Portfolio stop loss hit: Current price {current_price:.2f} is {abs(percent_change):.2f}% below entry price {self.entry_price:.2f}")
            return True
        
        return False
    
    def get_all_position_labels(self):
        """Get all position and order labels"""
        return list(set(list(self.mirrored_positions.keys()) + list(self.mirrored_orders.keys())))
    
    def clear_positions(self):
        """Clear all mirrored positions, orders and reset entry price"""
        self.mirrored_positions = {}
        self.mirrored_orders = {}
        self.entry_price = None
        logging.info("Cleared all mirrored positions and orders")
        log_to_sqlite('INFO', "Cleared all mirrored positions and orders")

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
        self.order_manager = OrderManager('pending_orders_AC2.json')
        self.reconnect_attempt = 0
        self.max_reconnect_attempts = 10
        self.base_delay = 1  # Base delay in seconds
        self.fresh_prices_event = asyncio.Event()

        self.socket_manager = SocketManager([5035, 5036])  # Added initialization
        
        # New OHLC calculator and position mirror
        self.ohlc_calculator = OHLCCalculator()  # Using global CANDLE_PERIOD_MINUTES
        self.position_mirror = PositionMirror()
        self.mirroring_active = False  # Flag to track if mirroring has been activated
        
        # Reset OHLC calculator for new session
        logging.info("Initializing new trading session - resetting OHLC calculator")
        log_to_sqlite('INFO', "Initializing new trading session - resetting OHLC calculator")
        self.ohlc_calculator.reset_for_new_session()

    async def wait_for_account_auth(self):
        while not self.account_authorized.is_set():
            try:
                await asyncio.wait_for(self.account_authorized.wait(), timeout=30)
            except asyncio.TimeoutError:
                logging.warning("Timed out waiting for account authorization. Retrying...")
                log_to_sqlite('WARNING', "Timed out waiting for account authorization. Retrying...")
                # You might want to trigger a reconnection here if it hasn't been triggered already
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
                await asyncio.sleep(delay)

    async def continuous_pending_order_check(self):
        while True:
            try:
                # Skip processing if socket manager is paused
                if hasattr(self, 'socket_manager') and self.socket_manager.paused:
                    await asyncio.sleep(1)
                    continue
                    
                pending_orders = self.order_manager.get_pending_orders()
                if pending_orders:
                    logging.info(f"Found {len(pending_orders)} pending orders to process")
                    for label, order in list(pending_orders.items()):  # Create a copy of the items to iterate over
                        # Check if the order is still pending (may have been processed by another task)
                        if label not in self.order_manager.get_pending_orders():
                            logging.info(f"Order {label} is no longer pending. Skipping.")
                            continue
                            
                        # Check if order is already being processed by another task
                        processing_orders = self.order_manager.get_processing_orders()
                        if label in processing_orders:
                            logging.info(f"Order {label} is already being processed by another task. Skipping.")
                            log_to_sqlite('INFO', f"Order {label} is already being processed by another task. Skipping.")
                            continue
                        
                        # Refresh both positions and orders
                        await self.refresh_positions_and_orders()
                        
                        # Check both existing orders and positions with same direction
                        existing_orders = [ord for ord in self.open_orders 
                                         if ord.tradeData.label == label and 
                                         ord.tradeData.tradeSide == ProtoOATradeSide.Value(order['signal'].upper())]
                        
                        existing_positions = [pos for pos in self.open_positions 
                                            if pos.tradeData.label == label and 
                                            pos.tradeData.tradeSide == ProtoOATradeSide.Value(order['signal'].upper())]
                        
                        # Check for opposite positions/orders that need to be closed first
                        opposite_orders = [ord for ord in self.open_orders 
                                         if ord.tradeData.label == label and 
                                         ord.tradeData.tradeSide != ProtoOATradeSide.Value(order['signal'].upper())]
                        
                        opposite_positions = [pos for pos in self.open_positions 
                                            if pos.tradeData.label == label and 
                                            pos.tradeData.tradeSide != ProtoOATradeSide.Value(order['signal'].upper())]
                        
                        if opposite_orders or opposite_positions:
                            logging.info(f"Found opposite direction orders/positions for {label}, closing them first")
                            await self.cancel_orders_and_positions(label)
                            # Let the next iteration handle placing the new order
                            continue
                        
                        if not existing_orders and not existing_positions:
                            logging.info(f"No existing order or position found for {label} in same direction, attempting to process")
                            await self.process_specific_pending_order(label, order)
                        else:
                            if existing_orders:
                                logging.info(f"Found existing order for {label} in same direction, skipping")
                                self.order_manager.remove_order(label)
                            if existing_positions:
                                logging.info(f"Found existing position for {label} in same direction, skipping")
                                self.order_manager.remove_order(label)
                
                await asyncio.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                logging.error(f"Error in continuous_pending_order_check: {e}")
                log_to_sqlite('ERROR', f"Error in continuous_pending_order_check: {e}")
                await asyncio.sleep(5)

    async def monitor_share_price(self):
        """Monitor WeightedSharePrice from TradingData_STR_AC2.db and check TP/SL"""
        while True:
            try:
                # Fetch latest data
                timestamp, price = self.ohlc_calculator.fetch_latest_data()
                
                if timestamp and price:
                    # Update OHLC candle
                    self.ohlc_calculator.update_candle(timestamp, price)
                    
                    # If mirroring is not active, check if rip threshold is met
                    if not self.mirroring_active and self.ohlc_calculator.check_rip_threshold():
                        logging.info("Rip threshold met, starting position mirroring")
                        log_to_sqlite('INFO', "Rip threshold met, starting position mirroring")
                        
                        # Mirror positions with current price as entry point for portfolio-wide TP/SL
                        # Pass strategies for volume reference
                        if self.position_mirror.mirror_positions(price, self.strategies):
                            # Add mirrored positions to pending orders first
                            for label, position in self.position_mirror.mirrored_positions.items():
                                # Get strategy config for this label if available
                                strategy = next((s for s in self.strategies if s['label'] == label), None)
                                
                                # Skip if no strategy found for this label
                                if not strategy:
                                    logging.warning(f"No strategy configuration found for {label}, skipping pending order creation")
                                    continue
                                    
                                # Create order data to add to pending orders (mirrored volume means same signal)
                                signal = "BUY" if position.get('Volume', 0) > 0 else "SELL"
                                
                                # Make sure SL/TP values are positive
                                sl_price = position.get('CurrentSL')
                                tp_price = position.get('CurrentTP')
                                
                                # Verify SL/TP are valid positive numbers
                                if sl_price is not None and (sl_price <= 0 or sl_price == ''):
                                    sl_price = None
                                if tp_price is not None and (tp_price <= 0 or tp_price == ''):
                                    tp_price = None
                                
                                order_data = {
                                    'signal': signal,
                                    'entry_price': position.get('EntryPrice'),
                                    'stop_loss_price': sl_price,
                                    'take_profit_price': tp_price,
                                    'timestamp': datetime.datetime.now().isoformat(),
                                    'prices_calculated': True,
                                    'symbol_id': strategy.get('symbol_id'),
                                    'volume': abs(position.get('Volume', 0)), # Use absolute value
                                    'symbol_precision': strategy.get('symbol_precision')
                                }
                                
                                # Add to pending orders
                                self.order_manager.add_order(label, order_data)
                                logging.info(f"Added mirrored position {label} to pending_orders_AC2.json")
                                
                            # Do the same for mirrored orders
                            for label, order in self.position_mirror.mirrored_orders.items():
                                # Skip processing if needed
                                # Similar logic as above for orders
                                pass
                                
                            self.mirroring_active = True
                            # Start the watchdog observers now that mirroring is active
                            for observer in self.observers:
                                if not observer.is_alive():
                                    observer.start()
                            logging.info("Signal watchers started - rip session is now active")
                            log_to_sqlite('INFO', "Signal watchers started - rip session is now active")
                            # Process the pending orders
                            await self.process_pending_orders()
                    
                    # If mirroring is active, check portfolio-wide TP/SL
                    elif self.mirroring_active and self.position_mirror.check_portfolio_tp_sl(price):
                        # Get all position labels from StrategyConfigAC2 to close
                        strategy_labels = [s['label'] for s in self.strategies]
                        logging.info(f"Portfolio-wide TP/SL hit. Closing all {len(strategy_labels)} positions and orders from StrategyConfigAC2")
                        log_to_sqlite('INFO', f"Portfolio-wide TP/SL hit. Closing all {len(strategy_labels)} positions and orders from StrategyConfigAC2")
                        
                        # Close all positions and orders for each strategy
                        for label in strategy_labels:
                            await self.cancel_orders_and_positions(label)
                        
                        # Reset mirroring and set cooldown until end of current candle
                        self.position_mirror.clear_positions()
                        self.mirroring_active = False
                        
                        # Stop all watchdog observers
                        for observer in self.observers:
                            if observer.is_alive():
                                observer.stop()
                                observer.join()
                        logging.info("Signal watchers stopped - rip session ended")
                        log_to_sqlite('INFO', "Signal watchers stopped - rip session ended")
                        
                        # Set cooldown until the end of the current candle
                        self.ohlc_calculator.set_cooldown_until_next_candle()
                        logging.info("Cooldown set after portfolio-wide TP/SL to prevent re-entry in same candle")
                        log_to_sqlite('INFO', "Cooldown set after portfolio-wide TP/SL to prevent re-entry in same candle")
                
                # Log candle info periodically
                current_candle = self.ohlc_calculator.get_current_candle()
                if current_candle['open'] is not None:
                    logging.info(f"Current 25m OHLC: O:{current_candle['open']:.2f} H:{current_candle['high']:.2f} L:{current_candle['low']:.2f} C:{current_candle['close']:.2f}")
                
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logging.error(f"Error in monitor_share_price: {e}")
                log_to_sqlite('ERROR', f"Error in monitor_share_price: {e}")
                await asyncio.sleep(30)  # Longer delay on error

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
        # Start sockets initially
        self.socket_manager.create_sockets()

        # Periodic task for restarting sockets every 10 minutes (600 seconds)
        asyncio.create_task(self.periodic_socket_restart(180))

        while True:
            try:
                await self.wait_for_account_auth()
                tasks = [
                    asyncio.create_task(self.process_signals()),
                    asyncio.create_task(self.continuous_pending_order_check()),
                    asyncio.create_task(self.check_fresh_quotes_for_pending_orders()),
                    asyncio.create_task(self.wait_and_process_fresh_prices()),
                    asyncio.create_task(self.monitor_share_price()),  # Add the new monitoring task
                ]
                await asyncio.gather(*tasks)
            except Exception as e:
                logging.error(f"Error in main loop: {e}")
                log_to_sqlite('ERROR', f"Error in main loop: {e}")
                await asyncio.sleep(5)
    
    async def periodic_socket_restart(self, interval):
        """Periodically restart the sockets every `interval` seconds."""
        while True:
            await asyncio.sleep(interval)  # Wait for the specified interval (e.g., 600 seconds)
            logging.info("Initiating socket restart...")
            self.socket_manager.restart_sockets()  # Restart sockets
            
    def start_signal_monitoring(self):
        # Signal monitoring is initialized but observers aren't started until mirroring_active becomes true
        for strategy in self.strategies:
            event_handler = SignalHandler(strategy, self.signal_queue)
            observer = Observer()
            observer.schedule(event_handler, path=os.path.dirname(strategy['signal_file']), recursive=False)
            # Don't start observers yet - they'll be started when mirroring becomes active
            self.observers.append(observer)
        logging.info("Signal watchers initialized but not started - waiting for active rip session")
        log_to_sqlite('INFO', "Signal watchers initialized but not started - waiting for active rip session")

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
                await asyncio.sleep(5)

    async def process_pending_orders(self):
        try:
            logging.info("BEGIN: Processing all pending orders")
            log_to_sqlite('INFO', "BEGIN: Processing all pending orders")

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
        finally:
            logging.info("END: Processing all pending orders")
            log_to_sqlite('INFO', "END: Processing all pending orders")

    async def check_pending_orders(self):
        while True:
            await self.process_pending_orders()
            await asyncio.sleep(1)  # Check every 5 minutes as a fallback
            
    def connected(self, client):
        logging.info("Connected")
        log_to_sqlite('INFO', "Connected")
        request = ProtoOAApplicationAuthReq()
        request.clientId = CLIENT_ID
        request.clientSecret = CLIENT_SECRET
        deferred = send_request_with_timeout(client, request)
        deferred.addCallback(self.onApplicationAuth)
        deferred.addErrback(self.onError)
        logging.info("Connected")
        self.reconnect_attempt = 0  # Reset the reconnection attempt counter

    def onApplicationAuth(self, response):
        logging.info("API Application authorized")
        log_to_sqlite('INFO', "API Application authorized")
        self.sendProtoOAAccountAuthReq()

    def disconnected(self, client, reason):
        logging.warning(f"Disconnected: {reason}")
        log_to_sqlite('WARNING', f"Disconnected: {reason}")
        self.loop.call_soon_threadsafe(self.account_authorized.clear)
        self.connect_with_retry()

    def connect_with_retry(self):
        if self.reconnect_attempt < self.max_reconnect_attempts:
            delay = min(self.calculate_delay(), 60)  # Cap delay at 60 seconds
            log_message = f"Attempting to connect (attempt {self.reconnect_attempt + 1}) in {delay:.2f} seconds..."
            logging.info(log_message)
            log_to_sqlite('INFO', log_message)
            
            def attempt_connection():
                try:
                    self.client.startService()
                    # If successful, the connected callback will reset reconnect_attempt
                except Exception as e:
                    error_message = f"Reconnection attempt {self.reconnect_attempt + 1} failed: {e}"
                    logging.error(error_message)
                    log_to_sqlite('ERROR', error_message)
                    self.reconnect_attempt += 1
                    self.connect_with_retry()  # Schedule next attempt

            reactor.callLater(delay, attempt_connection)
        else:
            error_message = "Max reconnection attempts reached. Please check your network connection."
            logging.error(error_message)
            log_to_sqlite('ERROR', error_message)
            # Here you might want to implement a more drastic recovery mechanism,
            # such as restarting the entire script or notifying an administrator

    def calculate_delay(self):
        return self.base_delay * (2 ** self.reconnect_attempt) + (random.randint(0, 1000) / 1000.0)
            
        logging.error("Max reconnection attempts reached. Please check your network connection.")
        log_to_sqlite('ERROR', "Max reconnection attempts reached. Please check your network connection.")

    def onMessageReceived(self, client, message):
        if message.payloadType == ProtoOAAccountAuthRes().payloadType:
            protoOAAccountAuthRes = Protobuf.extract(message)
            logging.info(f"Account {protoOAAccountAuthRes.ctidTraderAccountId} has been authorized")
            log_to_sqlite('INFO', f"Account {protoOAAccountAuthRes.ctidTraderAccountId} has been authorized")
            self.loop.call_soon_threadsafe(self.account_authorized.set)
            self.sendProtoOAReconcileReq()
        elif message.payloadType == ProtoOAReconcileRes().payloadType:
            protoOAReconcileRes = Protobuf.extract(message)
            self.open_positions = protoOAReconcileRes.position if hasattr(protoOAReconcileRes, 'position') else []
            self.open_orders = protoOAReconcileRes.order if hasattr(protoOAReconcileRes, 'order') else []
            logging.info("Open Positions and Orders reconciled")
            log_to_sqlite('INFO', "Open Positions and Orders reconciled")
        elif message.payloadType == ProtoOACancelOrderReq().payloadType:
            logging.info("Order has been canceled successfully.")
            log_to_sqlite('INFO', "Order has been canceled successfully.")
        elif message.payloadType == ProtoOAErrorRes().payloadType:
            protoOAErrorRes = Protobuf.extract(message)
            logging.error(f"Error received: {protoOAErrorRes.errorCode} - {protoOAErrorRes.description}")
            log_to_sqlite('ERROR', f"Error received: {protoOAErrorRes.errorCode} - {protoOAErrorRes.description}")
        else:
            logging.info(f"Message received: {Protobuf.extract(message)}")
            log_to_sqlite('INFO', f"Message received: {Protobuf.extract(message)}")

    def onError(self, failure):
        logging.error(f"Message Error: {failure}")
        log_to_sqlite('ERROR', f"Message Error: {failure}")
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
            request.volume = int(volume * 100)  # Convert volume to the correct format
            request.label = label  # Move this line here, before sending the request

            # Set the limit or stop price with the correct precision if applicable
            if request.orderType == ProtoOAOrderType.LIMIT and price is not None:
                request.limitPrice = round(float(price), symbol_precision)
            elif request.orderType == ProtoOAOrderType.STOP and price is not None:
                request.stopPrice = round(float(price), symbol_precision)
            
            # Set stop loss price with the correct precision if provided and positive
            if stop_loss_price is not None and float(stop_loss_price) > 0:
                request.stopLoss = round(float(stop_loss_price), symbol_precision)
            
            # Set take profit price with the correct precision if provided and positive
            if take_profit_price is not None and float(take_profit_price) > 0:
                request.takeProfit = round(float(take_profit_price), symbol_precision)

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
                        if execution_event.executionType == ProtoOAExecutionType.ORDER_ACCEPTED:
                            logging.info(f"Order has been accepted successfully for {label}")
                            log_to_sqlite('INFO', f"Order has been accepted successfully for {label}")
                            return True
                        else:
                            logging.warning(f"Unexpected execution type for {label}: {execution_event.executionType}")
                            log_to_sqlite('WARNING', f"Unexpected execution type for {label}: {execution_event.executionType}")
                            return False
                    else:
                        logging.error(f"Unexpected execution event type for {label}: {type(execution_event)}")
                        log_to_sqlite('ERROR', f"Unexpected execution event type for {label}: {type(execution_event)}")
                        return False
                else:
                    logging.error(f"Unexpected response type for {label}: {type(response)}")
                    log_to_sqlite('ERROR', f"Unexpected response type for {label}: {type(response)}")
                    return False
            except Exception as e:
                logging.error(f"Error placing order for {label}: {str(e)}")
                log_to_sqlite('ERROR', f"Error placing order for {label}: {str(e)}")
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
                return True
            except Exception as e:
                logging.error(f"Error canceling order {order_id} (attempt {attempt + 1}): {e}")
                log_to_sqlite('ERROR', f"Error canceling order {order_id} (attempt {attempt + 1}): {e}")
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
                return True
            except Exception as e:
                logging.error(f"Error closing position {position_id} (attempt {attempt + 1}): {e}")
                log_to_sqlite('ERROR', f"Error closing position {position_id} (attempt {attempt + 1}): {e}")
                if attempt < max_attempts - 1:
                    wait_time = (attempt + 1) * 2  # Increases wait time with each attempt
                    await asyncio.sleep(wait_time)
        return False

    async def cancel_orders_and_positions(self, label):
        attempt = 0  # We can still track the number of attempts for logging purposes.
        while True:
            try:
                await self.refresh_positions_and_orders()

                positions_to_close = [pos for pos in self.open_positions if pos.tradeData.label == label]
                orders_to_cancel = [ord for ord in self.open_orders if ord.tradeData.label == label]

                position_results = await asyncio.gather(*[self.close_position(pos.positionId, pos.tradeData.volume) for pos in positions_to_close], return_exceptions=True)
                order_results = await asyncio.gather(*[self.cancel_order(ord.orderId) for ord in orders_to_cancel], return_exceptions=True)

                await asyncio.sleep(2)
                await self.refresh_positions_and_orders()

                remaining_positions = [pos for pos in self.open_positions if pos.tradeData.label == label]
                remaining_orders = [ord for ord in self.open_orders if ord.tradeData.label == label]

                if not remaining_positions and not remaining_orders:
                    logging.info(f"All positions and orders for label {label} closed/canceled successfully")
                    log_to_sqlite('INFO', f"All positions and orders for label {label} closed/canceled successfully")
                    return True

                attempt += 1
                logging.warning(f"Attempt {attempt} to close all positions and cancel all orders for {label} failed. Retrying...")
                log_to_sqlite('WARNING', f"Attempt {attempt} to close all positions and cancel all orders for {label} failed. Retrying...")
                await asyncio.sleep(2)

            except asyncio.TimeoutError:
                logging.warning(f"Timeout refreshing positions and orders (attempt {attempt + 1})")
                log_to_sqlite('WARNING', f"Timeout refreshing positions and orders (attempt {attempt + 1})")
                await asyncio.sleep(5)  # Wait before retrying
            except Exception as e:
                logging.error(f"Error in cancel_orders_and_positions: {e}")
                log_to_sqlite('ERROR', f"Error in cancel_orders_and_positions: {e}")
                await asyncio.sleep(5)  # Wait before retrying


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
                return  # Success, exit the function
            except asyncio.TimeoutError:
                logging.error(f"Timeout while refreshing positions and orders (attempt {attempt + 1})")
                log_to_sqlite('ERROR', f"Timeout while refreshing positions and orders (attempt {attempt + 1})")
            except Exception as e:
                logging.error(f"Error refreshing positions and orders (attempt {attempt + 1}): {e}")
                log_to_sqlite('ERROR', f"Error refreshing positions and orders (attempt {attempt + 1}): {e}")
            
            if attempt < max_attempts - 1:
                wait_time = (attempt + 1) * 2
                await asyncio.sleep(wait_time)
        
        # If we've exhausted all attempts, raise an exception
        raise Exception("Failed to refresh positions and orders after multiple attempts")
        
    def get_recent_signal(self, signal_file):
        try:
            spec = importlib.util.spec_from_file_location("signal_module", signal_file)
            signal_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(signal_module)
            original_signal = signal_module.Action
            
            # For sell-the-rip strategy: keep original signal as is
            # When price rips up, original signal should be "sell" to sell the rip
            # When price dips down, original signal should be "buy" but we don't want to buy the dip
            inverted_signal = original_signal
                
            return inverted_signal
        except Exception as e:
            logging.error(f"Error reading signal from {signal_file}: {e}")
            log_to_sqlite('ERROR', f"Error reading signal from {signal_file}: {e}")
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

            if time_diff > datetime.timedelta(seconds=10):
                self.handle_stale_quotes(strategy, current_datetime)
                return False
            return True
        except Exception as e:
            logging.error(f"Error checking quote liveness for {strategy['label']}: {e}")
            log_to_sqlite('ERROR', f"Error checking quote liveness for {strategy['label']}: {e}")
            return False

    def handle_stale_quotes(self, strategy, current_datetime):
        warning_message = f"{current_datetime.strftime('%m/%d/%Y %H:%M:%S')}, Strategy: \"{strategy['label']}\", WARNING: Quotes not live. Waiting for market to come back online."
        print(warning_message)
        log_to_sqlite('WARNING', warning_message)
        self.loop.call_later(10, lambda: self.check_new_signal(strategy))

    def handle_new_signal(self, strategy, signal):
        print(f"New signal detected for {strategy['label']}: {signal}")
        log_to_sqlite('INFO', f"New signal detected for {strategy['label']}: {signal}")
        strategy['last_signal'] = signal
        if self.account_authorized.is_set():
            self.sendProtoOAReconcileReq()
        else:
            print("Account is not yet authorized. Adding signal to queue.")
            log_to_sqlite('INFO', "Account is not yet authorized. Adding signal to queue.")
            self.signal_queue.put((strategy, signal))

    async def process_signal(self, signal, strategy):
        try:
            logging.info(f"BEGIN: Processing signal for {strategy['label']}: {signal}")
            log_to_sqlite('INFO', f"BEGIN: Processing signal for {strategy['label']}: {signal}")

            quotes_fresh = self.are_quotes_live(strategy)
            logging.info(f"Quotes fresh for {strategy['label']}: {quotes_fresh}")
            log_to_sqlite('INFO', f"Quotes fresh for {strategy['label']}: {quotes_fresh}")

            entry_price = stop_loss_price = take_profit_price = None

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

                    # Calculate SL with positive value check
                    sl_percentage = strategy['stop_loss_percentage']
                    if sl_percentage > 0:
                        stop_loss_price = round(entry_price * (1 - sl_percentage / 100), strategy['symbol_precision'])
                        # Ensure it's positive
                        if stop_loss_price <= 0:
                            stop_loss_price = None
                    else:
                        stop_loss_price = None
                    
                    # Calculate TP with positive value check
                    tp_percentage = strategy['take_profit_percentage']
                    if tp_percentage > 0:
                        take_profit_price = round(entry_price * (1 + tp_percentage / 100), strategy['symbol_precision'])
                        # Ensure it's positive
                        if take_profit_price <= 0:
                            take_profit_price = None
                    else:
                        take_profit_price = None
                
                elif signal.lower() == "sell":
                    if strategy['sell_offset_percentage'] < 0:
                        entry_price = round(best_bid - abs(strategy['sell_offset_percentage']) / 100, strategy['symbol_precision'])
                    else:
                        entry_price = round(best_ask * (1 + strategy['sell_offset_percentage'] / 100), strategy['symbol_precision'])

                    # Calculate SL with positive value check
                    sl_percentage = strategy['stop_loss_percentage']
                    if sl_percentage > 0:
                        stop_loss_price = round(entry_price * (1 + sl_percentage / 100), strategy['symbol_precision'])
                        # Ensure it's positive
                        if stop_loss_price <= 0:
                            stop_loss_price = None
                    else:
                        stop_loss_price = None
                    
                    # Calculate TP with positive value check
                    tp_percentage = strategy['take_profit_percentage']
                    if tp_percentage > 0:
                        take_profit_price = round(entry_price * (1 - tp_percentage / 100), strategy['symbol_precision'])
                        # Ensure it's positive
                        if take_profit_price <= 0:
                            take_profit_price = None
                    else:
                        take_profit_price = None
                
                else:
                    logging.warning(f"Unknown signal for {strategy['label']}: {signal}")
                    log_to_sqlite('WARNING', f"Unknown signal for {strategy['label']}: {signal}")
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
            self.order_manager.add_order(strategy['label'], order_data)
            logging.info(f"Added pending order for {strategy['label']}: {order_data}")
            log_to_sqlite('INFO', f"Added pending order for {strategy['label']}: {order_data}")

            # Attempt to process this specific order immediately
            await self.process_specific_pending_order(strategy['label'], order_data)

            return True

        except Exception as e:
            logging.error(f"Error processing signal for {strategy['label']}: {e}")
            log_to_sqlite('ERROR', f"Error processing signal for {strategy['label']}: {e}")
            return False
        finally:
            logging.info(f"END: Processing signal for {strategy['label']}")
            log_to_sqlite('INFO', f"END: Processing signal for {strategy['label']}")

    async def process_specific_pending_order(self, label, order):
        try:
            logging.info(f"BEGIN: Processing specific pending order for {label}")
            log_to_sqlite('INFO', f"BEGIN: Processing specific pending order for {label}")

            # Check if the order is already being processed by another task
            processing_orders = self.order_manager.get_processing_orders()
            if label in processing_orders:
                logging.info(f"Order {label} is already being processed by another task. Skipping.")
                log_to_sqlite('INFO', f"Order {label} is already being processed by another task. Skipping.")
                return

            if not self.account_authorized.is_set():
                logging.warning(f"Account not authorized. Skipping pending order for {label}")
                log_to_sqlite('WARNING', f"Account not authorized. Skipping pending order for {label}")
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
                    return

            # Mark order as processing before canceling existing orders to prevent race conditions
            self.order_manager.mark_order_processing(label)

            await self.cancel_orders_and_positions(label)

            # Double-check that the order hasn't been processed by another task in the meantime
            if label not in self.order_manager._read_file():
                logging.info(f"Order {label} has been processed by another task. Skipping.")
                log_to_sqlite('INFO', f"Order {label} has been processed by another task. Skipping.")
                return

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
            else:
                # If order placement fails, mark it back as pending
                self.order_manager.add_order(label, order)
                logging.warning(f"Failed to process pending order for {label}, will retry later")
                log_to_sqlite('WARNING', f"Failed to process pending order for {label}, will retry later")

        except Exception as e:
            logging.error(f"Error processing specific pending order for {label}: {e}")
            log_to_sqlite('ERROR', f"Error processing specific pending order for {label}: {e}")
        finally:
            logging.info(f"END: Processing specific pending order for {label}")
            log_to_sqlite('INFO', f"END: Processing specific pending order for {label}")

    async def process_pending_orders(self):
        try:
            logging.info("BEGIN: Processing all pending orders")
            log_to_sqlite('INFO', "BEGIN: Processing all pending orders")

            pending_orders = self.order_manager.get_pending_orders()
            if not pending_orders:
                logging.info("No pending orders to process")
                log_to_sqlite('INFO', "No pending orders to process")
                return

            # Process each order one at a time to avoid race conditions
            for label, order in pending_orders.items():
                # Get fresh pending orders to avoid processing an order that's already being handled
                if label not in self.order_manager.get_pending_orders():
                    logging.info(f"Order {label} is no longer pending. Skipping.")
                    log_to_sqlite('INFO', f"Order {label} is no longer pending. Skipping.")
                    continue
                    
                # Check if the order is already being processed by another task
                processing_orders = self.order_manager.get_processing_orders()
                if label in processing_orders:
                    logging.info(f"Order {label} is already being processed by another task. Skipping.")
                    log_to_sqlite('INFO', f"Order {label} is already being processed by another task. Skipping.")
                    continue

                if not self.account_authorized.is_set():
                    logging.warning(f"Account not authorized. Skipping pending order for {label}")
                    log_to_sqlite('WARNING', f"Account not authorized. Skipping pending order for {label}")
                    continue

                if not order.get('prices_calculated', False):
                    logging.info(f"Prices not yet calculated for {label}, attempting to calculate now")
                    log_to_sqlite('INFO', f"Prices not yet calculated for {label}, attempting to calculate now")
                    strategy = next((s for s in self.strategies if s['label'] == label), None)
                    if strategy and self.are_quotes_live(strategy):
                        await self.calculate_prices_for_pending_order(strategy, order)
                    else:
                        logging.info(f"Unable to calculate prices for {label}, skipping for now")
                        log_to_sqlite('INFO', f"Unable to calculate prices for {label}, skipping for now")
                        continue

                # Mark order as processing before canceling existing orders to prevent race conditions
                self.order_manager.mark_order_processing(label)

                await self.cancel_orders_and_positions(label)

                # Double-check that the order hasn't been processed by another task in the meantime
                if label not in self.order_manager._read_file():
                    logging.info(f"Order {label} has been processed by another task. Skipping.")
                    log_to_sqlite('INFO', f"Order {label} has been processed by another task. Skipping.")
                    continue

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
                else:
                    # If order placement fails, mark it back as pending
                    self.order_manager.add_order(label, order)
                    logging.warning(f"Failed to process pending order for {label}, will retry later")
                    log_to_sqlite('WARNING', f"Failed to process pending order for {label}, will retry later")

        except Exception as e:
            logging.error(f"Error in process_pending_orders: {e}")
            log_to_sqlite('ERROR', f"Error in process_pending_orders: {e}")
        finally:
            logging.info("END: Processing all pending orders")
            log_to_sqlite('INFO', "END: Processing all pending orders")

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
                    self.fresh_prices_event.set()  # Signal that fresh prices are available
                await asyncio.sleep(1)
            except Exception as e:
                logging.error(f"Error in check_fresh_quotes_for_pending_orders: {e}")
                log_to_sqlite('ERROR', f"Error in check_fresh_quotes_for_pending_orders: {e}")
                await asyncio.sleep(5)

    async def wait_and_process_fresh_prices(self):
        while True:
            try:
                await self.fresh_prices_event.wait()
                # Check if there are any pending orders that haven't been processed yet
                pending_orders = self.order_manager.get_pending_orders()
                if pending_orders:
                    logging.info(f"Fresh prices available. Processing {len(pending_orders)} pending orders.")
                    log_to_sqlite('INFO', f"Fresh prices available. Processing {len(pending_orders)} pending orders.")
                    await self.process_pending_orders()
                self.fresh_prices_event.clear()  # Reset the event
            except Exception as e:
                logging.error(f"Error in wait_and_process_fresh_prices: {e}")
                log_to_sqlite('ERROR', f"Error in wait_and_process_fresh_prices: {e}")
                await asyncio.sleep(5)
        
    async def calculate_prices_for_pending_order(self, strategy, order):
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

            # Calculate SL with positive value check
            sl_percentage = strategy['stop_loss_percentage']
            if sl_percentage > 0:
                stop_loss_price = round(entry_price * (1 - sl_percentage / 100), strategy['symbol_precision'])
                # Ensure it's positive
                if stop_loss_price <= 0:
                    stop_loss_price = None
            else:
                stop_loss_price = None
            
            # Calculate TP with positive value check
            tp_percentage = strategy['take_profit_percentage']
            if tp_percentage > 0:
                take_profit_price = round(entry_price * (1 + tp_percentage / 100), strategy['symbol_precision'])
                # Ensure it's positive
                if take_profit_price <= 0:
                    take_profit_price = None
            else:
                take_profit_price = None
        
        elif signal.lower() == "sell":
            if strategy['sell_offset_percentage'] < 0:
                entry_price = round(best_bid - abs(strategy['sell_offset_percentage']) / 100, strategy['symbol_precision'])
            else:
                entry_price = round(best_ask * (1 + strategy['sell_offset_percentage'] / 100), strategy['symbol_precision'])

            # Calculate SL with positive value check
            sl_percentage = strategy['stop_loss_percentage']
            if sl_percentage > 0:
                stop_loss_price = round(entry_price * (1 + sl_percentage / 100), strategy['symbol_precision'])
                # Ensure it's positive
                if stop_loss_price <= 0:
                    stop_loss_price = None
            else:
                stop_loss_price = None
            
            # Calculate TP with positive value check
            tp_percentage = strategy['take_profit_percentage']
            if tp_percentage > 0:
                take_profit_price = round(entry_price * (1 - tp_percentage / 100), strategy['symbol_precision'])
                # Ensure it's positive
                if take_profit_price <= 0:
                    take_profit_price = None
            else:
                take_profit_price = None

        order['entry_price'] = entry_price
        order['stop_loss_price'] = stop_loss_price
        order['take_profit_price'] = take_profit_price
        order['prices_calculated'] = True
        self.order_manager.add_order(strategy['label'], order)  # Update the order in the OrderManager

        logging.info(f"Prices calculated for pending order {strategy['label']}: Entry: {entry_price}, SL: {stop_loss_price}, TP: {take_profit_price}")
        log_to_sqlite('INFO', f"Prices calculated for pending order {strategy['label']}: Entry: {entry_price}, SL: {stop_loss_price}, T{take_profit_price}")

def main():
    set_terminal_title("EMS-STR-AC2")
    max_restarts = 5
    restart_count = 0

    while restart_count < max_restarts:
        try:
            trading_system = TradingSystem(strategies)
            
            reactor_thread = threading.Thread(target=reactor.run, args=(False,))
            reactor_thread.start()
            
            trading_system.start()
        except KeyboardInterrupt:
            logging.info("Shutting down...")
            log_to_sqlite('INFO', "Shutting down...")
            break
        except Exception as e:
            logging.error(f"Unexpected error in main loop: {e}")
            log_to_sqlite('ERROR', f"Unexpected error in main loop: {e}")
            restart_count += 1
            logging.info(f"Attempting to restart (attempt {restart_count}/{max_restarts})...")
            log_to_sqlite('INFO', f"Attempting to restart (attempt {restart_count}/{max_restarts})...")
            time.sleep(10)  # Wait before restarting
        finally:
            if 'trading_system' in locals():
                # Clean up observers, ensuring they're properly stopped
                for observer in trading_system.observers:
                    try:
                        if observer.is_alive():
                            observer.stop()
                            logging.info(f"Observer stopped during shutdown")
                    except Exception as e:
                        logging.error(f"Error stopping observer during shutdown: {e}")
                        log_to_sqlite('ERROR', f"Error stopping observer during shutdown: {e}")
                
                for observer in trading_system.observers:
                    try:
                        if observer.is_alive():
                            observer.join()
                            logging.info(f"Observer joined during shutdown")
                    except Exception as e:
                        logging.error(f"Error joining observer during shutdown: {e}")
                        log_to_sqlite('ERROR', f"Error joining observer during shutdown: {e}")
            
            if 'reactor_thread' in locals():
                reactor.callFromThread(reactor.stop)
                reactor_thread.join()
            if 'trading_system' in locals() and trading_system.loop:
                trading_system.loop.close()

    if restart_count == max_restarts:
        logging.error("Max restart attempts reached. Please check the logs and restart the script manually.")
        log_to_sqlite('ERROR', "Max restart attempts reached. Please check the logs and restart the script manually.")

if __name__ == "__main__":
    main()
