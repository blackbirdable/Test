#!/usr/bin/env python
import csv
from datetime import datetime
import time
import os
import sys
from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
from ctrader_open_api.messages.OpenApiMessages_pb2 import *
from twisted.internet import reactor, defer
from StrategyConfigAC1 import strategies, CLIENT_ID, CLIENT_SECRET, ACCESS_TOKEN, ACCOUNT_ID
from threading import Lock, Thread
from queue import Queue
import logging
import random
import ctypes
import gc
import subprocess
import uuid
from google.cloud import bigquery
from google.oauth2 import service_account

def set_terminal_title(title):
    if sys.platform == 'win32':
        ctypes.windll.kernel32.SetConsoleTitleW(title)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# BigQuery configuration
PROJECT_ID = 'massive-clone-400221'  # Your actual project ID
DATASET_ID = 'financial_data'
TABLE_ID = 'market_quotes'
KEY_PATH = r'C:\Users\Administrator\Desktop\Sonixen\Tools\Ctrader OpenAPI\massive-clone-400221-a39952ec190a.json'  # Path to your service account key file

# Initialize BigQuery client
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# Keep local directory for logs
PY_DIRECTORY = r'C:\Users\Administrator\Desktop\Sonixen\Logs'
os.makedirs(PY_DIRECTORY, exist_ok=True)

RESTART_INTERVAL = 86000  # seconds
MAX_RESTART_INTERVAL = 300  # Maximum restart interval (5 minutes)
MAX_CONSECUTIVE_FAILURES = 50  # Number of consecutive failures before restarting the script

client = None
connection_state = "DISCONNECTED"
symbol_subscribers = {}
py_queue = Queue()
bq_queue = Queue()
consecutive_failures = 0

def read_symbol_ids():
    symbols = {}
    try:
        with open('Symbol_ID.csv', 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                symbol_id = int(row['SymbolID'])
                symbol_name = row['Name'].strip()
                symbols[symbol_id] = symbol_name
    except (FileNotFoundError, ValueError, KeyError) as e:
        logging.error(f"Error reading Symbol_ID.csv: {str(e)}")
        sys.exit(1)
    
    if not symbols:
        logging.error("Error: No symbols were read from the CSV file.")
        sys.exit(1)
    
    return symbols

SYMBOLS = read_symbol_ids()

class SymbolSubscriber:
    def __init__(self, symbol_id, symbol_name, py_queue, bq_queue):
        self.symbol_id = symbol_id
        self.symbol_name = symbol_name
        self.py_filename = os.path.join(PY_DIRECTORY, f"{symbol_name}_Quotes.py")
        self.last_quote = None
        self.py_queue = py_queue
        self.bq_queue = bq_queue
        self.lock = Lock()
        self.skip_count = 0
        self.skip_threshold = 100
        self.last_log_time = 0
        self.log_interval = 300

    def process_depth_quotes(self, depth_event):
        current_time = datetime.now().isoformat()
        
        best_bid_price = max((quote.bid for quote in depth_event.newQuotes if quote.HasField('bid') and quote.bid > 0), default=None)
        best_ask_price = min((quote.ask for quote in depth_event.newQuotes if quote.HasField('ask') and quote.ask > 0), default=None)
        
        if best_bid_price is not None and best_ask_price is not None:
            best_bid_price /= 100000
            best_ask_price /= 100000
            with self.lock:
                # Store data for both py files and BigQuery
                self.last_quote = (current_time, best_bid_price, best_ask_price)
            
            # Write to py files
            self.write_quote_to_py()
            
            # Send to BigQuery
            self.send_quote_to_bigquery()
            
            self.skip_count = 0
            
            current_time = time.time()
            if current_time - self.last_log_time > self.log_interval:
                logging.info(f"Successfully processed depth quote for {self.symbol_name}")
                self.last_log_time = current_time
        else:
            self.skip_count += 1
            if self.skip_count == self.skip_threshold:
                logging.warning(f"Skipped {self.skip_count} updates for {self.symbol_name} due to zero or missing bid/ask")

    def write_quote_to_py(self):
        with self.lock:
            if self.last_quote:
                self.py_queue.put((self.py_filename, self.last_quote))

    def send_quote_to_bigquery(self):
        with self.lock:
            if self.last_quote:
                # For BigQuery, we need to include symbol_name and symbol_id in the data
                timestamp, bid, ask = self.last_quote
                bq_data = (timestamp, bid, ask, self.symbol_name, self.symbol_id)
                self.bq_queue.put(bq_data)

def py_writer_thread(py_queue):
    while True:
        item = py_queue.get()
        if item[0] is None:
            break
        filename, data = item
        with open(filename, 'w') as file:
            file.write(f"timestamp = '{data[0]}'\n")
            file.write(f"bid = {data[1]}\n")
            file.write(f"ask = {data[2]}\n")
        py_queue.task_done()

def bigquery_writer_thread(queue):
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Create table if it doesn't exist
    try:
        bq_client.get_table(table_ref)
        logging.info(f"Table {table_ref} already exists")
    except Exception:
        schema = [
            bigquery.SchemaField("quote_id", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("bid", "FLOAT64"),
            bigquery.SchemaField("ask", "FLOAT64"),
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("symbol_id", "INTEGER")
        ]
        
        table = bigquery.Table(table_ref, schema=schema)
        # Add time partitioning
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="timestamp"
        )
        # Add clustering on symbol for faster queries
        table.clustering_fields = ["symbol"]
        
        table = bq_client.create_table(table)
        logging.info(f"Created table {table_ref}")
    
    # Process quotes in batches
    batch_size = 100
    quotes_batch = []
    
    while True:
        try:
            data = queue.get()
            if data is None:
                # Process any remaining quotes before exiting
                if quotes_batch:
                    upload_to_bigquery(quotes_batch, table_ref)
                break
                
            timestamp, bid, ask, symbol, symbol_id = data
            
            # Create a row for BigQuery
            row = {
                "quote_id": str(uuid.uuid4()),
                "timestamp": timestamp,
                "bid": bid,
                "ask": ask,
                "symbol": symbol,
                "symbol_id": symbol_id
            }
            
            quotes_batch.append(row)
            
            # If we've reached batch size, upload to BigQuery
            if len(quotes_batch) >= batch_size:
                upload_to_bigquery(quotes_batch, table_ref)
                quotes_batch = []
                
            queue.task_done()
        except Exception as e:
            logging.error(f"Error in BigQuery writer thread: {str(e)}")

def upload_to_bigquery(rows, table_ref):
    try:
        errors = bq_client.insert_rows_json(table_ref, rows)
        if errors:
            logging.error(f"BigQuery insert errors: {errors}")
        else:
            logging.info(f"Successfully inserted {len(rows)} rows to BigQuery")
    except Exception as e:
        logging.error(f"Error uploading to BigQuery: {e}")

def connected(client):
    global connection_state, consecutive_failures
    logging.info("Connected to server")
    connection_state = "CONNECTED"
    consecutive_failures = 0  # Reset consecutive failures on successful connection
    request = ProtoOAApplicationAuthReq(clientId=CLIENT_ID, clientSecret=CLIENT_SECRET)
    client.send(request).addErrback(onError)

def onMessageReceived(client, message):
    if message.payloadType == ProtoOAApplicationAuthRes().payloadType:
        logging.info("API Application authorized")
        connection_state = "APP_AUTHORIZED"
        sendProtoOAAccountAuthReq(client)
    elif message.payloadType == ProtoOAAccountAuthRes().payloadType:
        logging.info("Account authorized")
        connection_state = "ACCOUNT_AUTHORIZED"
        subscribeToDepthQuotes(client)
    elif message.payloadType == ProtoOADepthEvent().payloadType:
        depth_event = Protobuf.extract(message)
        symbol_id = depth_event.symbolId
        if symbol_id in symbol_subscribers:
            symbol_subscribers[symbol_id].process_depth_quotes(depth_event)
    elif message.payloadType == ProtoOAUnsubscribeDepthQuotesRes().payloadType:
        logging.info("Received unsubscribe confirmation from server")

def subscribeToDepthQuotes(client):
    global connection_state
    for symbol_id, symbol_name in SYMBOLS.items():
        logging.info(f"Subscribing to Depth Quotes for {symbol_name} (ID: {symbol_id})")
        request = ProtoOASubscribeDepthQuotesReq(ctidTraderAccountId=ACCOUNT_ID, symbolId=[symbol_id])
        client.send(request).addErrback(onError)
    connection_state = "SUBSCRIBED"

def unsubscribeFromDepthQuotes(client):
    logging.info("Unsubscribing from all depth quotes")
    request = ProtoOAUnsubscribeDepthQuotesReq(ctidTraderAccountId=ACCOUNT_ID, symbolId=list(SYMBOLS.keys()))
    return client.send(request)

def onError(failure):
    global consecutive_failures
    error_message = failure.getErrorMessage()
    logging.error(f"Error: {error_message}")
    consecutive_failures += 1
    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
        logging.error(f"Reached {MAX_CONSECUTIVE_FAILURES} consecutive failures. Restarting the script.")
        restart_script()
    else:
        logging.info("Attempting to reconnect...")
        reactor.callLater(5, perform_unsubscribe_and_restart)

def sendProtoOAAccountAuthReq(client):
    request = ProtoOAAccountAuthReq(ctidTraderAccountId=ACCOUNT_ID, accessToken=ACCESS_TOKEN)
    client.send(request).addErrback(onError)

@defer.inlineCallbacks
def perform_unsubscribe_and_restart(attempt=0):
    global client, connection_state
    logging.info(f"Initiating unsubscribe and restart process (attempt {attempt+1})...")
    
    if client and connection_state == "SUBSCRIBED":
        try:
            yield unsubscribeFromDepthQuotes(client)
            logging.info("Successfully unsubscribed from depth quotes")
        except Exception as e:
            logging.error(f"Error unsubscribing from depth quotes: {str(e)}")
    
    yield restart_loop(attempt)

@defer.inlineCallbacks
def restart_loop(attempt=0):
    global client, connection_state
    logging.info(f"Restarting the loop (attempt {attempt+1})...")
    
    if client:
        client.stopService()
    
    connection_state = "DISCONNECTED"
    
    client = Client(EndPoints.PROTOBUF_DEMO_HOST, EndPoints.PROTOBUF_PORT, TcpProtocol)
    client.setConnectedCallback(connected)
    client.setMessageReceivedCallback(onMessageReceived)
    yield defer.maybeDeferred(client.startService)
    
    next_interval = min(RESTART_INTERVAL * (2 ** attempt), MAX_RESTART_INTERVAL)
    next_interval += random.uniform(0, 5)
    reactor.callLater(next_interval, perform_unsubscribe_and_restart, attempt + 1)

def restart_script():
    logging.info("Restarting the script...")
    python = sys.executable
    os.execl(python, python, *sys.argv)

def main():
    global symbol_subscribers, client, py_queue, bq_queue
    
    set_terminal_title("QuoteCatcher")
    
    logging.info("Starting QuoteCatcher (Merged Version)...")
    logging.info(f"Symbols loaded: {SYMBOLS}")
    
    # Start py file writer thread
    py_thread = Thread(target=py_writer_thread, args=(py_queue,))
    py_thread.daemon = True
    py_thread.start()
    
    # Start BigQuery writer thread
    bq_thread = Thread(target=bigquery_writer_thread, args=(bq_queue,))
    bq_thread.daemon = True
    bq_thread.start()

    # Create symbol subscribers with both queues
    symbol_subscribers = {
        symbol_id: SymbolSubscriber(symbol_id, symbol_name, py_queue, bq_queue) 
        for symbol_id, symbol_name in SYMBOLS.items()
    }

    client = Client(EndPoints.PROTOBUF_DEMO_HOST, EndPoints.PROTOBUF_PORT, TcpProtocol)
    client.setConnectedCallback(connected)
    client.setMessageReceivedCallback(onMessageReceived)
    client.startService()

    reactor.callLater(RESTART_INTERVAL, perform_unsubscribe_and_restart)

    try:
        reactor.run()
    finally:
        # Clean shutdown of threads
        py_queue.put((None, None))
        bq_queue.put(None)
        py_thread.join()
        bq_thread.join()

if __name__ == "__main__":
    main()
