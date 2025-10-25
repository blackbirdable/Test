import logging
from flask import Flask, jsonify, request
import datetime
import csv
from pytz import timezone, utc
import os
import ctypes
import threading
import queue
import time
import json
import traceback
from google.cloud import bigquery
from google.oauth2 import service_account

def set_terminal_title(title):
    ctypes.windll.kernel32.SetConsoleTitleW(title)

# Suppress Flask's default request log
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

app = Flask(__name__)

# Define the log directory
LOG_DIR = r"C:\Users\Administrator\Desktop\Sonixen\Logs"

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
            print(f"Dataset {dataset_id} already exists")
        except Exception:
            dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
            dataset.location = "US"
            dataset = client.create_dataset(dataset, timeout=30)
            print(f"Created dataset {dataset_id}")
        
        # Create OMS table if it doesn't exist
        create_oms_table(client)
        
        return client
    except Exception as e:
        print(f"Error initializing BigQuery client: {e}")
        logging.error(f"BigQuery client initialization error: {e}\n{traceback.format_exc()}")
        return None

def create_oms_table(client):
    """Create OMS table if it doesn't exist."""
    oms_schema = [
        bigquery.SchemaField("Timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("Strategy", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Action", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Price", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("RawMessage", "STRING"),
        bigquery.SchemaField("ProcessingDelay", "FLOAT"),
        bigquery.SchemaField("QueueType", "STRING"),
        bigquery.SchemaField("SignalCount", "INTEGER"),
        bigquery.SchemaField("ProcessedTimestamp", "TIMESTAMP"),
        bigquery.SchemaField("AdditionalData", "STRING")
    ]
    
    table_id = f"{project_id}.{dataset_id}.OMS"
    try:
        client.get_table(table_id)
        print("Table OMS already exists")
    except Exception:
        table = bigquery.Table(table_id, schema=oms_schema)
        table = client.create_table(table)
        print(f"Created table {table.table_id}")

def log_to_bigquery(bq_client, strategy, timestamp, action, price, raw_message=None, 
                   processing_delay=None, queue_type=None, signal_count=None, additional_data=None):
    """Log signal data to BigQuery OMS table."""
    if not bq_client:
        print("BigQuery client not available - skipping BigQuery logging")
        return
    
    try:
        # Convert timestamp string to proper format for BigQuery
        if isinstance(timestamp, str):
            timestamp_obj = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            timestamp_obj = timestamp_obj.replace(tzinfo=utc)
        else:
            timestamp_obj = timestamp
            
        processed_timestamp = datetime.datetime.now(utc)
        
        # Ensure price is a valid float
        try:
            price_float = float(price)
        except (ValueError, TypeError):
            price_float = 0.0
            # Add note about price conversion to additional_data
            if additional_data is None:
                additional_data = {}
            additional_data['original_price'] = str(price)
            if str(strategy) == 'HEARTBEAT':
                additional_data['message_type'] = 'heartbeat_ping'
            else:
                additional_data['price_conversion_note'] = 'Non-numeric price converted to 0.0'
        
        row_data = {
            "Timestamp": timestamp_obj.strftime('%Y-%m-%d %H:%M:%S'),
            "Strategy": str(strategy),
            "Action": str(action).upper(),
            "Price": price_float,
            "RawMessage": str(raw_message) if raw_message else None,
            "ProcessingDelay": float(processing_delay) if processing_delay is not None else None,
            "QueueType": str(queue_type) if queue_type else None,
            "SignalCount": int(signal_count) if signal_count is not None else None,
            "ProcessedTimestamp": processed_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            "AdditionalData": json.dumps(additional_data) if additional_data else None
        }
        
        table_id = f"{project_id}.{dataset_id}.OMS"
        errors = bq_client.insert_rows_json(table_id, [row_data])
        
        if errors:
            print(f"Errors occurred while inserting OMS data to BigQuery: {errors}")
            logging.error(f"BigQuery OMS insert errors: {errors}")
        else:
            if str(strategy) == 'HEARTBEAT':
                print(f"Heartbeat logged to BigQuery: {action} @ {processed_timestamp.strftime('%H:%M:%S')}")
            else:
                print(f"Successfully logged to BigQuery: {strategy} - {action} @ {price_float}")
        
    except Exception as e:
        print(f"Error logging to BigQuery: {e}")
        logging.error(f"Error logging to BigQuery: {e}")
        logging.error(f"Traceback: {traceback.format_exc()}")

class SignalProcessor:
    def __init__(self, bq_client=None):
        self.bq_client = bq_client
        self.queues = {
            'AT_XAGUSD_240_MARKET': queue.Queue(),
            'default': queue.Queue()
        }
        self.signal_counts = {
            'AT_XAGUSD_240_MARKET': 0,
            'default': 0
        }
        self.start_processors()

    def start_processors(self):
        threading.Thread(target=self.process_market_queue, daemon=True).start()
        threading.Thread(target=self.process_default_queue, daemon=True).start()

    def process_market_queue(self):
        while True:
            # Get initial signal
            data = self.queues['AT_XAGUSD_240_MARKET'].get()
            signal_start_time = time.time()
            signals_collected = 1
            self.signal_counts['AT_XAGUSD_240_MARKET'] += 1
            
            # Wait 7 seconds and collect any new signals that arrive
            time.sleep(7)
            
            # Get the most recent signal if any new ones arrived
            while not self.queues['AT_XAGUSD_240_MARKET'].empty():
                data = self.queues['AT_XAGUSD_240_MARKET'].get()
                signals_collected += 1
                self.signal_counts['AT_XAGUSD_240_MARKET'] += 1
                self.queues['AT_XAGUSD_240_MARKET'].task_done()
            
            # Calculate processing delay
            processing_delay = time.time() - signal_start_time
            
            # Process the most recent signal
            self.process_signal(*data, processing_delay=processing_delay, 
                              queue_type='AT_XAGUSD_240_MARKET', signal_count=signals_collected)
            self.queues['AT_XAGUSD_240_MARKET'].task_done()

    def process_default_queue(self):
        while True:
            data = self.queues['default'].get()
            signal_start_time = time.time()
            self.signal_counts['default'] += 1
            
            # Minimal processing delay for default queue
            processing_delay = time.time() - signal_start_time
            
            self.process_signal(*data, processing_delay=processing_delay, 
                              queue_type='default', signal_count=1)
            self.queues['default'].task_done()

    def process_signal(self, strategy, timestamp, action, price, raw_message=None, 
                      processing_delay=None, queue_type=None, signal_count=None):
        file_write_success = True
        
        try:
            # Always process to files first (this should continue regardless of BigQuery status)
            append_to_csv(strategy, timestamp, action, price)
            append_to_py(strategy, timestamp, action, price)
            
            print(f"[{datetime.datetime.now(utc).strftime('%Y-%m-%d %H:%M:%S')}] "
                  f"{'Heartbeat received' if strategy == 'HEARTBEAT' else f'Processed signal: {strategy} - {action.upper()} @ {price}'} "
                  f"(Queue: {queue_type}, Delay: {processing_delay:.2f}s, Signals: {signal_count})")
                  
        except Exception as e:
            file_write_success = False
            print(f"Error writing signal files: {e}")
            logging.error(f"Error writing signal files: {e}")
        
        # Attempt BigQuery logging separately - this should not affect file writing
        try:
            if self.bq_client:
                additional_data = {
                    'total_signals_processed': self.signal_counts.get(queue_type, 0),
                    'file_write_success': file_write_success
                }
                
                log_to_bigquery(self.bq_client, strategy, timestamp, action, price, 
                               raw_message=raw_message, processing_delay=processing_delay,
                               queue_type=queue_type, signal_count=signal_count,
                               additional_data=additional_data)
        except Exception as e:
            print(f"Error logging to BigQuery (continuing with file processing): {e}")
            logging.error(f"BigQuery logging error (file processing continues): {e}")

def append_to_csv(strategy, timestamp, action, price):
    filename = f"{strategy}.csv"
    filepath = os.path.join(LOG_DIR, filename)
    
    os.makedirs(LOG_DIR, exist_ok=True)
    
    with open(filepath, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([timestamp, action, price])

def append_to_py(strategy, timestamp, action, price):
    filename = f"{strategy}.py"
    filepath = os.path.join(LOG_DIR, filename)
    
    os.makedirs(LOG_DIR, exist_ok=True)
    
    with open(filepath, 'w', encoding='utf-8') as file:
        file.write(f"# Signal file for {strategy}\n\n")
        file.write(f"Action = '{action.upper()}'\n")
        file.write(f"Timestamp = '{timestamp}'\n")
        file.write(f"Price = {price}\n")

    log_filename = f"{strategy}_log.py"
    log_filepath = os.path.join(LOG_DIR, log_filename)
    
    with open(log_filepath, 'a+', encoding='utf-8') as file:
        file.seek(0)
        if not file.read(1):
            file.write(f"# Log of signals for {strategy}\n\n")
            file.write("from datetime import datetime\n\n")
            file.write("signals = [\n")
        else:
            file.seek(0, os.SEEK_END)
            position = file.tell() - 1
            file.seek(position)
            last_char = file.read(1)
            if last_char != '[' and last_char != ',':
                file.write(',\n')
            else:
                file.write('\n')
        
        file.write("    {\n")
        file.write(f"        'Timestamp': '{timestamp}',\n")
        file.write(f"        'Action': '{action.upper()}',\n")
        file.write(f"        'Price': {price},\n")
        file.write(f"        'TimestampObject': datetime.strptime('{timestamp}', '%Y-%m-%d %H:%M:%S'),\n")
        file.write("    }")
        
    with open(log_filepath, 'r+', encoding='utf-8') as file:
        file.seek(0, os.SEEK_END)
        position = file.tell() - 1
        file.seek(position)
        last_char = file.read(1)
        if last_char != ']':
            file.write('\n]')

def extract_strategy(message):
    # Handle PING messages specifically
    if message.strip().upper() == 'PING':
        return 'HEARTBEAT'
    
    strategy = message.split()[0]
    return strategy

def extract_action(message):
    # Handle PING messages specifically
    if message.strip().upper() == 'PING':
        return 'PING'
    
    action_part = message.split('@')[0].split('-')[-1].strip().lower()
    return action_part

def extract_price(message):
    try:
        price_part = message.split('@')[-1].strip().split()[0]
        # Try to convert to float, return 0.0 if it fails
        return float(price_part)
    except (ValueError, IndexError):
        # Return 0.0 for non-numeric prices (like PING messages)
        return 0.0

# Initialize BigQuery client and signal processor
print("Initializing BigQuery client...")
bq_client = initialize_bigquery_client()
if bq_client:
    print("BigQuery client initialized successfully for OMS")
else:
    print("BigQuery client initialization failed - continuing without BigQuery logging")

signal_processor = SignalProcessor(bq_client)

@app.route('/webhook', methods=['POST'])
def webhook():
    webhook_received_time = time.time()
    utc_timestamp = datetime.datetime.now(utc).strftime('%Y-%m-%d %H:%M:%S')
    message = request.data.decode('utf-8')
    
    print(f"[{utc_timestamp}] Webhook received:")
    print(f"Message: {message}")
    
    try:
        strategy = extract_strategy(message)
        action = extract_action(message)
        price = extract_price(message)
        
        print(f"Parsed - Strategy: {strategy}, Action: {action}, Price: {price}")
        
        # Determine queue and add to appropriate processor
        queue_name = strategy if strategy == 'AT_XAGUSD_240_MARKET' else 'default'
        
        # Add webhook processing time to additional data
        webhook_data = {
            'webhook_received_time': webhook_received_time,
            'parsing_time': time.time() - webhook_received_time
        }
        
        signal_processor.queues[queue_name].put((strategy, utc_timestamp, action, price, message))
        
        print(f"Signal queued in '{queue_name}' queue")
        
        return jsonify({
            'status': 'success', 
            'strategy': strategy, 
            'action': action, 
            'price': price,
            'queue': queue_name,
            'timestamp': utc_timestamp
        }), 200
        
    except Exception as e:
        error_message = f"Error processing webhook: {e}"
        print(error_message)
        logging.error(f"{error_message}\n{traceback.format_exc()}")
        
        # Log error to BigQuery if possible (but don't let it stop processing)
        try:
            if bq_client:
                log_to_bigquery(bq_client, "UNKNOWN", utc_timestamp, "ERROR", 0.0,
                               raw_message=message, additional_data={'error': str(e)})
        except Exception as bq_error:
            print(f"Additional BigQuery error logging failed: {bq_error}")
        
        return jsonify({'status': 'error', 'message': str(e)}), 400

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint to monitor OMS status."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.datetime.now(utc).strftime('%Y-%m-%d %H:%M:%S'),
        'bigquery_connected': bq_client is not None,
        'queue_sizes': {
            'AT_XAGUSD_240_MARKET': signal_processor.queues['AT_XAGUSD_240_MARKET'].qsize(),
            'default': signal_processor.queues['default'].qsize()
        },
        'total_signals_processed': signal_processor.signal_counts
    }), 200

@app.route('/stats', methods=['GET'])
def get_stats():
    """Get processing statistics."""
    return jsonify({
        'queue_sizes': {
            'AT_XAGUSD_240_MARKET': signal_processor.queues['AT_XAGUSD_240_MARKET'].qsize(),
            'default': signal_processor.queues['default'].qsize()
        },
        'total_signals_processed': signal_processor.signal_counts,
        'bigquery_status': 'connected' if bq_client else 'disconnected',
        'timestamp': datetime.datetime.now(utc).strftime('%Y-%m-%d %H:%M:%S')
    }), 200

if __name__ == '__main__':
    set_terminal_title("OMS")
    print("="*60)
    print("OMS (Order Management System) Starting...")
    print(f"BigQuery Integration: {'Enabled' if bq_client else 'Disabled'}")
    print(f"Log Directory: {LOG_DIR}")
    print("="*60)
    
    try:
        app.run(debug=False, host='0.0.0.0', port=5000)
    except KeyboardInterrupt:
        print("\nShutting down OMS...")
        try:
            if bq_client:
                log_to_bigquery(bq_client, "SYSTEM", datetime.datetime.now(utc).strftime('%Y-%m-%d %H:%M:%S'), 
                               "SHUTDOWN", 0.0, additional_data={'reason': 'manual_shutdown'})
        except Exception as bq_error:
            print(f"BigQuery shutdown logging failed: {bq_error}")
    except Exception as e:
        print(f"Error running OMS: {e}")
        try:
            if bq_client:
                log_to_bigquery(bq_client, "SYSTEM", datetime.datetime.now(utc).strftime('%Y-%m-%d %H:%M:%S'), 
                               "ERROR", 0.0, additional_data={'error': str(e)})
        except Exception as bq_error:
            print(f"BigQuery error logging failed: {bq_error}")
