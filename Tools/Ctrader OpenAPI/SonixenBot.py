import asyncio
import csv
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
import os
import logging
from telethon import TelegramClient, events
import ctypes
import json
import psutil
import subprocess
import sqlite3
from typing import Optional, Tuple, Dict, Any, List
import google.cloud.bigquery as bigquery
from google.oauth2 import service_account
import sys

def set_terminal_title(title: str) -> None:
    """Set the terminal window title (Windows only)."""
    ctypes.windll.kernel32.SetConsoleTitleW(title)

@dataclass
class Config:
    """Configuration class for SonixenBot."""
    # Telegram Bot and chat details
    bot_token: str = os.getenv('BOT_TOKEN', '7126879357:AAE4q2ZBXfP8wdqW8z8HZ5PV2MB01A8iK7M')
    chat_id: int = int(os.getenv('CHAT_ID', '-4189612481'))
    api_id: int = int(os.getenv('API_ID', '21740566'))
    api_hash: str = os.getenv('API_HASH', '0ef40fead7c440a944767c8d4b18b272')
    
    # File paths
    ems_log_file: str = os.getenv('EMS_LOG_FILE', r'C:\Users\Administrator\Desktop\EMS_Log_Benchmark.db')
    btc_quotes_file: str = os.getenv('BTC_QUOTES_FILE', r'C:\Users\Administrator\Desktop\Sonixen\Logs\BTC_Quotes.py')
    trading_db_path: str = os.getenv('TRADING_DB_PATH', r'C:\Users\Administrator\Desktop\TradingData.db')
    previous_price_file: str = os.getenv('PREVIOUS_PRICE_FILE', r'C:\Users\Administrator\Desktop\PreviousDayPrice.json')
    ping_file: str = os.getenv('PING_FILE', r'C:\Users\Administrator\Desktop\Sonixen\Logs\HEARTBEAT.py')
    benchmark_portfolio_terminal_path: str = os.getenv('BENCHMARK_PORTFOLIO_TERMINAL_PATH', r'C:\Users\Administrator\Desktop\Sonixen\Tools\Terminals\Benchmark_Portfolio_Terminal.py')
    
    # BigQuery configuration
    bigquery_project_id: str = os.getenv('BIGQUERY_PROJECT_ID', 'massive-clone-400221')
    bigquery_dataset_id: str = os.getenv('BIGQUERY_DATASET_ID', 'benchmark_portfolio')
    bigquery_table_id: str = os.getenv('BIGQUERY_TABLE_ID', 'Summary')
    bigquery_credentials_path: str = os.getenv('BIGQUERY_CREDENTIALS_PATH', r'C:\Users\Administrator\Desktop\Sonixen\Tools\Ctrader OpenAPI\massive-clone-400221-a39952ec190a.json')
    
    # Thresholds
    ems_log_threshold: timedelta = timedelta(minutes=int(os.getenv('EMS_LOG_THRESHOLD_MINUTES', '3')))
    btc_quotes_threshold: timedelta = timedelta(minutes=int(os.getenv('BTC_QUOTES_THRESHOLD_MINUTES', '7')))
    quote_catcher_threshold: timedelta = timedelta(minutes=int(os.getenv('QUOTE_CATCHER_THRESHOLD_MINUTES', '10')))
    warning_cooldown: timedelta = timedelta(minutes=int(os.getenv('WARNING_COOLDOWN_MINUTES', '5')))
    ping_threshold: timedelta = timedelta(minutes=int(os.getenv('PING_THRESHOLD_MINUTES', '5')))
    pending_orders_threshold: timedelta = timedelta(hours=int(os.getenv('PENDING_ORDERS_THRESHOLD_HOURS', '1')))
    benchmark_portfolio_threshold: timedelta = timedelta(minutes=int(os.getenv('BENCHMARK_PORTFOLIO_THRESHOLD_MINUTES', '5')))
    
    # Constants
    year_start_price: float = float(os.getenv('YEAR_START_PRICE', '100.00'))

# Setup structured logging
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO').upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize configuration
config = Config()

# Initialize the TelegramClient
client = TelegramClient('bot_session', config.api_id, config.api_hash)

# Dictionary to store the last warning time for each check
last_warning_times: Dict[str, datetime] = {}

@contextmanager
def database_connection(db_path: str):
    """Context manager for database connections."""
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        yield conn
    except sqlite3.Error as e:
        logger.error(f"Database error with {db_path}: {e}")
        raise
    finally:
        if conn:
            conn.close()

class ProcessManager:
    """Handles process management operations."""
    
    @staticmethod
    def terminate_processes_by_name(process_name: str, script_name: str = None) -> List[int]:
        """Terminates all processes matching the given criteria."""
        terminated_pids = []
        for proc in psutil.process_iter(['name', 'cmdline', 'pid']):
            try:
                if (proc.info['name'] == process_name and 
                    (script_name is None or any(script_name in cmd for cmd in proc.info['cmdline']))):
                    proc.terminate()
                    logger.info(f"Terminated {script_name or process_name} process with PID {proc.info['pid']}")
                    proc.wait(timeout=5)
                    terminated_pids.append(proc.info['pid'])
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired) as e:
                logger.error(f"Error terminating process {proc.info['pid']}: {e}")
        return terminated_pids
    
    @staticmethod
    async def close_process_by_window_title(window_title: str, max_attempts: int = 3) -> bool:
        """Closes a process by its window title."""
        for attempt in range(max_attempts):
            closed = False
            for proc in psutil.process_iter(['pid']):
                try:
                    def enum_window_callback(hwnd, _):
                        if ctypes.windll.user32.IsWindowVisible(hwnd):
                            window_text = ctypes.create_unicode_buffer(1024)
                            ctypes.windll.user32.GetWindowTextW(hwnd, window_text, 1024)
                            if window_text.value == window_title:
                                window_pid = ctypes.c_ulong()
                                ctypes.windll.user32.GetWindowThreadProcessId(hwnd, ctypes.byref(window_pid))
                                if window_pid.value == proc.pid:
                                    proc.terminate()
                                    proc.wait(timeout=5)
                                    nonlocal closed
                                    closed = True
                        return True
                    
                    ctypes.windll.user32.EnumWindows(
                        ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_int, ctypes.c_int)(enum_window_callback), 
                        0
                    )
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired) as e:
                    logger.error(f"Error in process termination attempt {attempt + 1}: {e}")
            
            if closed:
                logger.info(f"{window_title} process terminated successfully")
                return True
            
            if attempt < max_attempts - 1:
                await asyncio.sleep(10)  # Wait before retry
        
        logger.error(f"Failed to close {window_title} process after {max_attempts} attempts")
        return False
    
    @staticmethod
    def start_process(script_path: str, working_dir: str = None) -> Optional[subprocess.Popen]:
        """Starts a new process for the given script."""
        if not os.path.exists(script_path):
            logger.error(f"Script not found: {script_path}")
            return None
        
        try:
            if working_dir:
                original_dir = os.getcwd()
                os.chdir(working_dir)
            
            process = subprocess.Popen(
                [sys.executable, script_path],
                creationflags=subprocess.CREATE_NEW_CONSOLE
            )
            
            if working_dir:
                os.chdir(original_dir)
            
            logger.info(f"Started process for {script_path} with PID: {process.pid}")
            return process
        except Exception as e:
            logger.error(f"Error starting process for {script_path}: {e}")
            return None

def terminate_quote_catcher_processes() -> List[int]:
    """Terminates all running QuoteCatcher processes."""
    return ProcessManager.terminate_processes_by_name('python.exe', 'QuoteCatcher.py')

async def send_message_via_telegram(chat_id: int, message: str) -> bool:
    """Asynchronously sends a message to a given chat ID via Telegram.
    
    Returns:
        bool: True if message was sent successfully, False otherwise.
    """
    try:
        await client.send_message(chat_id, message)
        logger.debug(f"Message sent successfully to chat {chat_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to send message via Telegram: {e}")
        return False

async def get_previous_day_price() -> Tuple[Optional[float], Optional[str]]:
    """Retrieves the previous day's closing price from a file.
    
    Returns:
        Tuple of (price, date) or (None, None) if file doesn't exist or is invalid.
    """
    try:
        with open(config.previous_price_file, 'r') as file:
            data = json.load(file)
            return data['price'], data['date']
    except FileNotFoundError:
        logger.warning(f"Previous price file not found: {config.previous_price_file}")
        return None, None
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Error reading previous day price file: {e}")
        return None, None

async def update_previous_day_price(price: float, date: str) -> bool:
    """Updates the previous day's closing price in a file.
    
    Returns:
        bool: True if update was successful, False otherwise.
    """
    try:
        with open(config.previous_price_file, 'w') as file:
            json.dump({'price': price, 'date': date}, file)
        logger.debug(f"Updated previous day price: {price} on {date}")
        return True
    except Exception as e:
        logger.error(f"Error updating previous day price file: {e}")
        return False

async def get_previous_day_closing_price_from_data(share_price_data: List[Dict[str, Any]]) -> Tuple[Optional[float], Optional[str]]:
    """Determines the closing price of the previous trading day from provided data.
    
    Args:
        share_price_data: List of dictionaries containing share price data
        
    Returns:
        Tuple of (price, timestamp) or (None, None) if no previous day data found
    """
    today = datetime.now().date()
    
    # Assuming share_price_data is sorted in descending order by timestamp
    for entry in share_price_data:
        try:
            entry_date = datetime.strptime(entry['Timestamp'], '%Y-%m-%d %H:%M:%S').date()
            if entry_date < today:
                return entry['SharePrice'], entry['Timestamp']
        except (KeyError, ValueError) as e:
            logger.warning(f"Invalid entry in share price data: {e}")
            continue
    
    return None, None

def get_previous_month_end() -> datetime:
    """Gets the last day of the previous month at 23:59:59."""
    today = datetime.now()
    first_day_of_current_month = today.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    return last_day_of_previous_month.replace(hour=23, minute=59, second=59)

async def read_latest_share_price(include_monthly: bool = False) -> Tuple[str, Optional[float], Optional[datetime], Optional[float]]:
    """Reads the latest share price from the database and optionally includes monthly and yearly comparison.
    
    Args:
        include_monthly: Whether to include monthly and yearly comparisons
        
    Returns:
        Tuple of (message, latest_price, latest_timestamp, previous_month_price)
    """
    try:
        with database_connection(config.trading_db_path) as conn:
            cursor = conn.cursor()
            
            # Get latest share price and timestamp
            cursor.execute('''
                SELECT Timestamp, WeightedSharePrice 
                FROM Summary 
                ORDER BY Timestamp DESC 
                LIMIT 1
            ''')
            latest_result = cursor.fetchone()
            
            if not latest_result:
                logger.warning("No share price data available in database")
                return "No share price data available in database.", None, None, None
                
            latest_timestamp = datetime.strptime(latest_result[0], '%Y-%m-%d %H:%M:%S')
            latest_price = latest_result[1]
            
            # Format the timestamp to show only hour and minute
            formatted_timestamp = latest_timestamp.strftime('%H:%M')
            
            # Get previous day's closing price
            prev_price, prev_timestamp = await get_previous_day_closing_price_from_db(cursor)
            
            message = f"Share Price: ${latest_price:.2f} (as of {formatted_timestamp})\n"
            
            if prev_price and prev_timestamp:
                await update_previous_day_price(prev_price, prev_timestamp)
                daily_percent_change = ((latest_price - prev_price) / prev_price) * 100
                message += f"Daily Change: {daily_percent_change:+.2f}% (${prev_price:.2f} → ${latest_price:.2f})\n"
            
            previous_month_price = None
            if include_monthly:
                previous_month_end = get_previous_month_end()
                
                # Get previous month's closing price
                cursor.execute('''
                    SELECT WeightedSharePrice
                    FROM Summary
                    WHERE Timestamp <= ?
                    ORDER BY Timestamp DESC
                    LIMIT 1
                ''', (previous_month_end.strftime('%Y-%m-%d %H:%M:%S'),))
                monthly_result = cursor.fetchone()
                
                if monthly_result:
                    previous_month_price = monthly_result[0]
                    monthly_percent_change = ((latest_price - previous_month_price) / previous_month_price) * 100
                    message += f"MTD Change: {monthly_percent_change:+.2f}% (${previous_month_price:.2f} → ${latest_price:.2f})\n"
                else:
                    message += "Previous month's closing price not available.\n"
                
                # Add yearly comparison
                yearly_percent_change = ((latest_price - config.year_start_price) / config.year_start_price) * 100
                message += f"YTD Change: {yearly_percent_change:+.2f}% (${config.year_start_price:.2f} → ${latest_price:.2f})"
            
            return message, latest_price, latest_timestamp, previous_month_price
            
    except Exception as e:
        logger.error(f"Failed to read latest share price from database: {e}")
        return "Unable to retrieve the latest share price from database.", None, None, None

async def get_previous_day_closing_price_from_db(cursor) -> Tuple[Optional[float], Optional[str]]:
    """Determines the closing price of the previous trading day from the database.
    
    Args:
        cursor: Database cursor
        
    Returns:
        Tuple of (price, timestamp) or (None, None) if no data found
    """
    today = datetime.now().date()
    
    try:
        cursor.execute('''
            SELECT Timestamp, WeightedSharePrice
            FROM Summary
            WHERE date(Timestamp) < ?
            ORDER BY Timestamp DESC
            LIMIT 1
        ''', (today.strftime('%Y-%m-%d'),))
        
        result = cursor.fetchone()
        if result:
            return result[1], result[0]
        
        logger.warning("No previous day closing price found in database")
        return None, None
    except sqlite3.Error as e:
        logger.error(f"Database error getting previous day closing price: {e}")
        return None, None

@client.on(events.NewMessage(pattern='(?i)(yearly|ytd|monthly|mtd|update)'))
async def consolidated_handler(event) -> None:
    """Handles 'yearly', 'ytd', 'monthly', 'mtd', and 'update' commands in Telegram."""
    try:
        share_price_message, _, _, _ = await read_latest_share_price(include_monthly=True)
        await client.send_message(event.chat_id, share_price_message)
        logger.info(f"Responded to {event.pattern_match.group(0)} command from chat {event.chat_id}")
    except Exception as e:
        logger.error(f"Error handling consolidated command: {e}")
        await client.send_message(event.chat_id, "Error retrieving share price information.")

@client.on(events.NewMessage(pattern='(?i)restart\\s+ems'))
async def restart_ems_handler(event) -> None:
    """Handles 'restart EMS' command to manually restart EMS-BENCHMARK.py."""
    try:
        logger.info(f"Manual EMS restart requested by chat {event.chat_id}")
        await client.send_message(event.chat_id, "Restarting EMS-BENCHMARK...")
        
        success = await restart_ems_process()
        
        if success:
            await client.send_message(event.chat_id, "✅ EMS-BENCHMARK has been restarted successfully.")
            logger.info(f"Manual EMS restart completed successfully for chat {event.chat_id}")
        else:
            await client.send_message(event.chat_id, "❌ Failed to restart EMS-BENCHMARK. Check logs for details.")
            logger.warning(f"Manual EMS restart failed for chat {event.chat_id}")
            
    except Exception as e:
        logger.error(f"Error handling restart EMS command: {e}")
        await client.send_message(event.chat_id, "❌ Error occurred while restarting EMS-BENCHMARK.")

@client.on(events.NewMessage(pattern='(?i)restart\\s+oms'))
async def restart_oms_handler(event) -> None:
    """Handles 'restart OMS' command to manually restart OMS.py."""
    try:
        logger.info(f"Manual OMS restart requested by chat {event.chat_id}")
        await client.send_message(event.chat_id, "Restarting OMS...")
        
        success = await restart_oms_process()
        
        if success:
            await client.send_message(event.chat_id, "✅ OMS has been restarted successfully.")
            logger.info(f"Manual OMS restart completed successfully for chat {event.chat_id}")
        else:
            await client.send_message(event.chat_id, "❌ Failed to restart OMS. Check logs for details.")
            logger.warning(f"Manual OMS restart failed for chat {event.chat_id}")
            
    except Exception as e:
        logger.error(f"Error handling restart OMS command: {e}")
        await client.send_message(event.chat_id, "❌ Error occurred while restarting OMS.")

@client.on(events.NewMessage(pattern='(?i)restart\\s+quotecatcher'))
async def restart_quotecatcher_handler(event) -> None:
    """Handles 'restart quotecatcher' command to manually restart QuoteCatcher.py."""
    try:
        logger.info(f"Manual QuoteCatcher restart requested by chat {event.chat_id}")
        await client.send_message(event.chat_id, "Restarting QuoteCatcher...")
        
        # Terminate existing processes
        terminated_pids = terminate_quote_catcher_processes()
        logger.info(f"Terminated {len(terminated_pids)} QuoteCatcher processes")
        
        # Start new QuoteCatcher process
        quote_catcher_path = r'C:\Users\Administrator\Desktop\Sonixen\Tools\Ctrader OpenAPI\QuoteCatcher.py'
        quote_catcher_dir = os.path.dirname(quote_catcher_path)
        
        process = ProcessManager.start_process(quote_catcher_path, quote_catcher_dir)
        
        if process:
            await client.send_message(event.chat_id, "✅ QuoteCatcher has been restarted successfully.")
            logger.info(f"Manual QuoteCatcher restart completed successfully for chat {event.chat_id}")
        else:
            await client.send_message(event.chat_id, "❌ Failed to restart QuoteCatcher. Check logs for details.")
            logger.warning(f"Manual QuoteCatcher restart failed for chat {event.chat_id}")
            
    except Exception as e:
        logger.error(f"Error handling restart QuoteCatcher command: {e}")
        await client.send_message(event.chat_id, "❌ Error occurred while restarting QuoteCatcher.")

@client.on(events.NewMessage(pattern='(?i)restart\\s+terminal'))
async def restart_terminal_handler(event) -> None:
    """Handles 'restart terminal' command to manually restart Benchmark Portfolio Terminal."""
    try:
        logger.info(f"Manual Benchmark Portfolio Terminal restart requested by chat {event.chat_id}")
        await client.send_message(event.chat_id, "Restarting Benchmark Portfolio Terminal...")
        
        success = await restart_benchmark_portfolio_terminal()
        
        if success:
            await client.send_message(event.chat_id, "✅ Benchmark Portfolio Terminal has been restarted successfully.")
            logger.info(f"Manual Benchmark Portfolio Terminal restart completed successfully for chat {event.chat_id}")
        else:
            await client.send_message(event.chat_id, "❌ Failed to restart Benchmark Portfolio Terminal. Check logs for details.")
            logger.warning(f"Manual Benchmark Portfolio Terminal restart failed for chat {event.chat_id}")
            
    except Exception as e:
        logger.error(f"Error handling restart Terminal command: {e}")
        await client.send_message(event.chat_id, "❌ Error occurred while restarting Benchmark Portfolio Terminal.")

@client.on(events.NewMessage(pattern='(?i)kill'))
async def kill_handler(event) -> None:
    """Handles 'kill' command to close EMS-BENCHMARK and then shutdown SonixenBot."""
    try:
        logger.warning(f"KILL command requested by chat {event.chat_id}")
        await client.send_message(event.chat_id, "⚠️ KILL command received. Shutting down EMS-BENCHMARK...")
        
        # First, close EMS-BENCHMARK
        ems_closed = await ProcessManager.close_process_by_window_title("EMS-BENCHMARK", max_attempts=3)
        
        if ems_closed:
            await client.send_message(event.chat_id, "✅ EMS-BENCHMARK closed successfully.")
            logger.info("EMS-BENCHMARK closed via KILL command")
        else:
            await client.send_message(event.chat_id, "⚠️ Could not confirm EMS-BENCHMARK closure, but proceeding with shutdown.")
            logger.warning("Could not confirm EMS-BENCHMARK closure")
        
        # Give a moment for the message to be sent
        await asyncio.sleep(1)
        
        # Send final shutdown message
        await client.send_message(event.chat_id, "🔴 SonixenBot shutting down now. Goodbye!")
        logger.warning("SonixenBot shutting down via KILL command")
        
        # Give time for final message to be sent
        await asyncio.sleep(2)
        
        # Trigger shutdown by raising an exception that will be caught in main()
        raise SystemExit("KILL command executed - shutting down")
        
    except SystemExit:
        # Re-raise SystemExit to ensure proper shutdown
        raise
    except Exception as e:
        logger.error(f"Error handling KILL command: {e}")
        await client.send_message(event.chat_id, "❌ Error during shutdown process.")

@client.on(events.NewMessage(pattern='(?i)(help|commands)'))
async def help_handler(event) -> None:
    """Handles 'help' or 'commands' to show available commands."""
    try:
        help_message = """
📋 **Available Commands:**

📊 **Price Commands:**
• `yearly` / `ytd` - Show year-to-date performance
• `monthly` / `mtd` - Show month-to-date performance  
• `update` - Show latest share price

🔄 **Restart Commands:**
• `restart ems` - Restart EMS-BENCHMARK
• `restart oms` - Restart OMS
• `restart quotecatcher` - Restart QuoteCatcher
• `restart terminal` - Restart Benchmark Portfolio Terminal

⚠️ **Control Commands:**
• `kill` - Close EMS-BENCHMARK and shutdown SonixenBot

❓ **Info Commands:**
• `help` / `commands` - Show this help message
        """.strip()
        
        await client.send_message(event.chat_id, help_message)
        logger.info(f"Help command executed for chat {event.chat_id}")
        
    except Exception as e:
        logger.error(f"Error handling help command: {e}")
        await client.send_message(event.chat_id, "❌ Error displaying help information.")

async def check_ping_file() -> bool:
    """Checks the HEARTBEAT.py file and restarts OMS if needed.
    
    Returns:
        bool: True if check passed, False if issues were found
    """
    if not os.path.exists(config.ping_file):
        logger.error(f"PING file not found: {config.ping_file}")
        await send_rate_limited_warning(config.chat_id, f"Warning: PING file not found at {config.ping_file}")
        return False

    try:
        with open(config.ping_file, 'r') as file:
            content = file.read()
            timestamp_lines = [line for line in content.split('\n') if 'Timestamp' in line]
            if not timestamp_lines:
                logger.error("No timestamp found in HEARTBEAT.py file")
                return False
                
            timestamp_str = timestamp_lines[0].split("'")[1]
            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        
        time_diff = datetime.now() - timestamp
        if time_diff > config.ping_threshold:
            logger.warning(f"HEARTBEAT.py is older than {config.ping_threshold}")
            await send_rate_limited_warning(config.chat_id, f"HEARTBEAT.py is older than {config.ping_threshold}.")
            await restart_oms_process()
            return False
        else:
            logger.debug(f"HEARTBEAT.py is up to date. Last update: {timestamp_str}")
            return True
            
    except (IndexError, ValueError, FileNotFoundError) as e:
        logger.error(f"Error parsing HEARTBEAT.py file: {e}")
        return False

async def restart_oms_process() -> bool:
    """Restarts the OMS process.
    
    Returns:
        bool: True if restart was successful, False otherwise
    """
    # Try to close OMS up to 3 times
    if await ProcessManager.close_process_by_window_title("OMS", max_attempts=3):
        await asyncio.sleep(3)  # Wait before starting new process
    
    # Verify no OMS processes are running and start new one
    oms_running = any(
        proc for proc in psutil.process_iter(['pid']) 
        if _check_window_title(proc.pid, "OMS")
    )
    
    if not oms_running:
        # Define the correct absolute path for OMS.py
        oms_path = r'C:\Users\Administrator\Desktop\Sonixen\Tools\Ctrader OpenAPI\OMS.py'
        if ProcessManager.start_process(oms_path):
            logger.info("Started new OMS.py")
            await send_rate_limited_warning(config.chat_id, "OMS has been restarted.")
            return True
        else:
            logger.error("Failed to start new OMS process")
            await send_rate_limited_warning(config.chat_id, "Failed to restart OMS: Could not start new process")
            return False
    else:
        logger.error("Failed to close existing OMS process")
        await send_rate_limited_warning(config.chat_id, "Failed to restart OMS: Could not close existing process")
        return False

def _check_window_title(pid: int, title: str) -> bool:
    """Helper function to check if a process has a specific window title."""
    try:
        window_text = ctypes.create_unicode_buffer(1024)
        if ctypes.windll.user32.GetWindowTextW(pid, window_text, 1024):
            return window_text.value == title
    except:
        pass
    return False
        
async def check_ems_log() -> bool:
    """Checks if EMS-BENCHMARK process is running and database is updated, restarts if needed.
    
    Returns:
        bool: True if check passed, False if issues were found
    """
    # First check if EMS-BENCHMARK process is running
    ems_running = _is_process_running('EMS-BENCHMARK.py')
    logger.debug(f"EMS-BENCHMARK process running: {ems_running}")
    
    # If not running, restart immediately
    if not ems_running:
        logger.warning("EMS-BENCHMARK process not running. Restarting immediately.")
        success = await restart_ems_process()
        if success:
            await send_rate_limited_warning(config.chat_id, "🔄 EMS-BENCHMARK was offline - restarted automatically.")
        return success
    
    # If running, check database freshness
    if not os.path.exists(config.ems_log_file):
        logger.error(f"EMS log database not found: {config.ems_log_file}")
        await send_rate_limited_warning(config.chat_id, f"Warning: EMS log database not found at {config.ems_log_file}")
        return False

    try:
        with database_connection(config.ems_log_file) as conn:
            cursor = conn.cursor()

            # Query the latest log entry
            cursor.execute('''
                SELECT timestamp 
                FROM logs 
                ORDER BY timestamp DESC 
                LIMIT 1
            ''')
            latest_entry = cursor.fetchone()

            if not latest_entry:
                logger.error("No log entries found in EMS_Log_Benchmark.db")
                await send_rate_limited_warning(config.chat_id, "Warning: No log entries found in EMS_Log_Benchmark.db")
                return False

            # Parse the timestamp from the database
            timestamp_str = latest_entry[0]
            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            current_time = datetime.now()
            time_diff = current_time - timestamp

            # Check if the log is older than the threshold
            if time_diff > config.ems_log_threshold:
                logger.warning(f"EMS_Log_Benchmark.db is older than {config.ems_log_threshold}")
                success = await restart_ems_process()
                if success:
                    await send_rate_limited_warning(config.chat_id, f"EMS-BENCHMARK restarted due to stale database. Last update: {timestamp_str}")
                return success
            else:
                logger.debug(f"EMS_Log_Benchmark.db is up to date. Last update: {timestamp_str}")
                return True
                
    except (sqlite3.Error, ValueError) as e:
        logger.error(f"Error checking EMS_Log_Benchmark.db: {e}")
        return False

async def restart_ems_process() -> bool:
    """Restarts the EMS-BENCHMARK process.
    
    Returns:
        bool: True if restart was successful, False otherwise
    """
    # Try to close EMS up to 3 times
    if await ProcessManager.close_process_by_window_title("EMS-BENCHMARK", max_attempts=3):
        await asyncio.sleep(3)  # Wait before starting new process
    
    # Verify no EMS processes are running and start new one
    ems_running = any(
        proc for proc in psutil.process_iter(['pid']) 
        if _check_window_title(proc.pid, "EMS-BENCHMARK")
    )
    
    if not ems_running:
        # Start new EMS process
        ems_path = os.path.join(os.path.dirname(__file__), 'EMS-BENCHMARK.py')
        if ProcessManager.start_process(ems_path):
            logger.info("Started new EMS-BENCHMARK.py")
            await send_rate_limited_warning(config.chat_id, "EMS-BENCHMARK has been restarted.")
            return True
        else:
            logger.error("Failed to start new EMS-BENCHMARK process")
            await send_rate_limited_warning(config.chat_id, "Failed to restart EMS-BENCHMARK: Could not start new process")
            return False
    else:
        logger.error("Failed to close existing EMS-BENCHMARK process")
        await send_rate_limited_warning(config.chat_id, "Failed to restart EMS-BENCHMARK: Could not close existing process")
        return False
 


def is_monitoring_time() -> bool:
    """Check if current time is within monitoring hours and days.
    
    Returns:
        bool: True if within monitoring hours, False otherwise
    """
    current_time = datetime.now()
    current_hour = current_time.hour
    is_weekend = current_time.weekday() >= 5  # 5 is Saturday, 6 is Sunday
    
    # Skip checks between 23:00 and 1:00, and on weekends
    if is_weekend or (current_hour >= 23 or current_hour < 1):
        logger.debug("Outside monitoring hours - skipping checks")
        return False
    return True

async def check_btc_quotes() -> bool:
    """Checks the last modification time of BTC_Quotes.py.
    
    Returns:
        bool: True if check passed (file is up to date), False otherwise
    """
    if not is_monitoring_time():
        return True

    if not os.path.exists(config.btc_quotes_file):
        logger.error(f"BTC Quotes file not found: {config.btc_quotes_file}")
        await send_rate_limited_warning(config.chat_id, f"Warning: BTC Quotes file not found at {config.btc_quotes_file}")
        return False

    try:
        last_modified_time = datetime.fromtimestamp(os.path.getmtime(config.btc_quotes_file))
        current_time = datetime.now()
        time_diff = current_time - last_modified_time

        if time_diff > config.btc_quotes_threshold:
            logger.warning(f"BTC_Quotes.py is older than {config.btc_quotes_threshold}")
            formatted_time = last_modified_time.strftime('%Y-%m-%d %H:%M:%S')
            await send_rate_limited_warning(config.chat_id, f"Warning: QuoteCatcher offline. Last updated at {formatted_time}")
            return False
        else:
            logger.debug(f"BTC_Quotes.py is up to date. Last update: {last_modified_time.strftime('%Y-%m-%d %H:%M:%S')}")
            return True

    except OSError as e:
        logger.error(f"Error checking BTC_Quotes.py file: {e}")
        return False

def _is_process_running(script_name: str) -> bool:
    """Check if a specific Python script process is currently running."""
    for proc in psutil.process_iter(['name', 'cmdline']):
        try:
            if (proc.info['name'] == 'python.exe' and 
                proc.info['cmdline'] and
                any(script_name in cmd for cmd in proc.info['cmdline'])):
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return False

async def check_and_restart_quote_catcher() -> bool:
    """Checks if QuoteCatcher process is running and BTC_Quotes.py is updated, restarts if needed.
    
    Returns:
        bool: True if check passed or restart successful, False if issues occurred
    """
    logger.debug("Starting QuoteCatcher check...")
    
    # First check if QuoteCatcher process is running
    quote_catcher_running = _is_process_running('QuoteCatcher.py')
    logger.debug(f"QuoteCatcher process running: {quote_catcher_running}")
    
    # If not running, restart immediately
    if not quote_catcher_running:
        logger.warning("QuoteCatcher process not running. Restarting immediately.")
        success = await _restart_quote_catcher_process()
        if success:
            await send_rate_limited_warning(config.chat_id, "🔄 QuoteCatcher was offline - restarted automatically.")
        return success
    
    # If running, check file freshness
    if not os.path.exists(config.btc_quotes_file):
        logger.error(f"BTC Quotes file not found: {config.btc_quotes_file}")
        await send_rate_limited_warning(config.chat_id, f"Warning: BTC Quotes file not found at {config.btc_quotes_file}")
        return False

    try:
        last_modified_time = datetime.fromtimestamp(os.path.getmtime(config.btc_quotes_file))
        current_time = datetime.now()
        time_diff = current_time - last_modified_time
        
        logger.debug(f"BTC_Quotes.py last modified: {last_modified_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.debug(f"Time difference: {time_diff}, Threshold: {config.quote_catcher_threshold}")

        if time_diff > config.quote_catcher_threshold:
            logger.warning(f"BTC_Quotes.py is older than {config.quote_catcher_threshold}. Restarting QuoteCatcher.")
            success = await _restart_quote_catcher_process()
            if success:
                await send_rate_limited_warning(config.chat_id, "QuoteCatcher has been restarted due to stale data.")
            return success
        else:
            logger.debug(f"BTC_Quotes.py is up to date. Last update: {last_modified_time.strftime('%Y-%m-%d %H:%M:%S')}")
            return True

    except OSError as e:
        logger.error(f"Error checking BTC_Quotes.py file: {e}")
        return False

async def _restart_quote_catcher_process() -> bool:
    """Restarts the QuoteCatcher process.
    
    Returns:
        bool: True if restart was successful, False otherwise
    """
    # Terminate all existing QuoteCatcher processes
    terminated_pids = terminate_quote_catcher_processes()
    logger.info(f"Terminated {len(terminated_pids)} QuoteCatcher processes")
    
    # Define QuoteCatcher.py path
    quote_catcher_path = r'C:\Users\Administrator\Desktop\Sonixen\Tools\Ctrader OpenAPI\QuoteCatcher.py'
    quote_catcher_dir = os.path.dirname(quote_catcher_path)
    
    if not os.path.exists(quote_catcher_path):
        logger.error(f"QuoteCatcher.py not found at: {quote_catcher_path}")
        await send_rate_limited_warning(config.chat_id, f"QuoteCatcher.py not found at: {quote_catcher_path}")
        return False
    
    # Launch the script
    process = ProcessManager.start_process(quote_catcher_path, quote_catcher_dir)
    if process:
        logger.info(f"Started QuoteCatcher process with PID: {process.pid}")
        return True
    else:
        logger.error("Failed to start QuoteCatcher process")
        await send_rate_limited_warning(config.chat_id, "Error starting QuoteCatcher")
        return False
        
async def send_rate_limited_warning(chat_id: int, warning_message: str) -> bool:
    """Sends a warning message to the chat, but limits it to once per cooldown period.
    
    Args:
        chat_id: Telegram chat ID
        warning_message: Message to send
        
    Returns:
        bool: True if message was sent, False if skipped due to cooldown
    """
    current_time = datetime.now()
    if (warning_message not in last_warning_times or 
        current_time - last_warning_times[warning_message] > config.warning_cooldown):
        success = await send_message_via_telegram(chat_id, warning_message)
        if success:
            last_warning_times[warning_message] = current_time
            logger.info(f"Sent warning: {warning_message}")
        return success
    else:
        logger.debug(f"Skipped sending warning due to cooldown: {warning_message}")
        return False

async def check_pending_orders() -> bool:
    """Checks if the first timestamp in pending_orders_AC1.json is older than threshold.
    
    Returns:
        bool: True if check passed, False if authorization issues detected
    """
    pending_orders_file = r'C:\Users\Administrator\Desktop\Sonixen\Tools\Ctrader OpenAPI\pending_orders_AC1.json'
    
    if not os.path.exists(pending_orders_file):
        logger.error(f"Pending orders file not found: {pending_orders_file}")
        await send_rate_limited_warning(config.chat_id, f"Warning: Pending orders file not found at {pending_orders_file}")
        return False

    try:
        with open(pending_orders_file, 'r') as file:
            orders_data = json.load(file)
            
        if not orders_data:
            logger.debug("No pending orders found")
            return True
            
        # Get the first timestamp from any order
        first_timestamp = None
        for order_data in orders_data.values():
            if 'timestamp' in order_data:
                try:
                    timestamp_str = order_data['timestamp']
                    current_timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%f')
                    if first_timestamp is None or current_timestamp < first_timestamp:
                        first_timestamp = current_timestamp
                except ValueError as e:
                    logger.warning(f"Invalid timestamp format in pending orders: {e}")
                    continue
        
        if first_timestamp:
            current_time = datetime.now()
            time_diff = current_time - first_timestamp
            if time_diff > config.pending_orders_threshold:
                logger.warning("Trading Account no longer Authorized")
                await send_rate_limited_warning(config.chat_id, "Trading Account no longer Authorized")
                return False
        
        return True

    except (json.JSONDecodeError, FileNotFoundError) as e:
        logger.error(f"Error checking pending_orders_AC1.json: {e}")
        return False

async def check_benchmark_portfolio_terminal() -> bool:
    """Checks the latest timestamp from BigQuery data for Benchmark Portfolio Terminal and restarts if needed.
    
    Returns:
        bool: True if check passed, False if issues were found
    """
    try:
        # Check if credentials file exists
        if not os.path.exists(config.bigquery_credentials_path):
            logger.error(f"BigQuery credentials file not found at {config.bigquery_credentials_path}")
            await send_rate_limited_warning(config.chat_id, f"Warning: BigQuery credentials file not found at {config.bigquery_credentials_path}")
            return False
            
        try:
            # Create BigQuery client with service account credentials
            credentials = service_account.Credentials.from_service_account_file(
                config.bigquery_credentials_path, 
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            
            bq_client = bigquery.Client(
                credentials=credentials,
                project=credentials.project_id or config.bigquery_project_id
            )
            
            # Discover available datasets and tables
            dataset_id, table_id = await _discover_bigquery_table(bq_client)
            if not dataset_id or not table_id:
                logger.error("Could not find appropriate BigQuery dataset/table")
                return False
            
            # Query to get the latest timestamp
            query = f"""
            SELECT MAX(Timestamp) as latest_timestamp
            FROM `{bq_client.project}.{dataset_id}.{table_id}`
            """
            
            logger.debug(f"Executing query on table: {bq_client.project}.{dataset_id}.{table_id}")
            
            # Execute the query
            query_job = bq_client.query(query)
            results = query_job.result()
            
            # Get the latest timestamp
            row = list(results)[0]
            latest_timestamp = row.latest_timestamp
            
            if not latest_timestamp:
                logger.error("No timestamp data found in BigQuery table")
                await send_rate_limited_warning(config.chat_id, "Warning: No timestamp data found for Benchmark Portfolio Terminal in BigQuery")
                return False
            
            # Check if the latest timestamp is older than the threshold
            current_time = datetime.now(latest_timestamp.tzinfo)
            time_diff = current_time - latest_timestamp
            
            if time_diff > config.benchmark_portfolio_threshold:
                logger.warning(f"Benchmark Portfolio Terminal data is older than {config.benchmark_portfolio_threshold}")
                formatted_time = latest_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                await send_rate_limited_warning(config.chat_id, f"Warning: Benchmark Portfolio Terminal may be offline. Last data update at {formatted_time}. Restarting terminal.")
                
                success = await restart_benchmark_portfolio_terminal()
                return success
            else:
                logger.debug(f"Benchmark Portfolio Terminal data is up to date. Last update: {latest_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
                return True
                
        except Exception as e:
            logger.error(f"BigQuery query error: {e}")
            await send_rate_limited_warning(config.chat_id, f"Error querying BigQuery: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Error checking Benchmark Portfolio Terminal: {e}")
        return False

async def _discover_bigquery_table(bq_client) -> Tuple[str, str]:
    """Discovers the appropriate BigQuery dataset and table.
    
    Returns:
        Tuple of (dataset_id, table_id) or (None, None) if not found
    """
    try:
        # Get a list of datasets
        datasets = list(bq_client.list_datasets())
        
        if datasets:
            datasets_str = ', '.join(dataset.dataset_id for dataset in datasets)
            logger.debug(f"Available datasets: {datasets_str}")
            
            # Try to find a dataset containing benchmark or portfolio
            benchmark_datasets = [d for d in datasets if 'benchmark' in d.dataset_id.lower() or 'portfolio' in d.dataset_id.lower()]
            if benchmark_datasets:
                dataset_id = benchmark_datasets[0].dataset_id
                logger.debug(f"Using dataset: {dataset_id}")
                
                # List tables in this dataset
                tables = list(bq_client.list_tables(dataset_id))
                if tables:
                    tables_str = ', '.join(table.table_id for table in tables)
                    logger.debug(f"Available tables in {dataset_id}: {tables_str}")
                    
                    # Try to find the Summary table first
                    summary_tables = [t for t in tables if t.table_id == 'Summary']
                    if summary_tables:
                        table_id = summary_tables[0].table_id
                    else:
                        # Fall back to other relevant tables
                        portfolio_tables = [t for t in tables if any(keyword in t.table_id.lower() 
                                          for keyword in ['portfolio', 'updates', 'data', 'benchmark'])]
                        if portfolio_tables:
                            table_id = portfolio_tables[0].table_id
                        else:
                            logger.warning(f"No matching tables found in dataset {dataset_id}")
                            return None, None
                    
                    logger.debug(f"Using table: {table_id}")
                    return dataset_id, table_id
                else:
                    logger.warning(f"No tables found in dataset {dataset_id}")
            else:
                # Use default values if no specific datasets found
                logger.info("Using default dataset and table configuration")
                return config.bigquery_dataset_id, config.bigquery_table_id
        else:
            logger.warning("No datasets found in the project")
            
    except Exception as e:
        logger.error(f"Error discovering BigQuery tables: {e}")
    
    return None, None

async def restart_benchmark_portfolio_terminal() -> bool:
    """Restarts the Benchmark Portfolio Terminal process.
    
    Returns:
        bool: True if restart was successful, False otherwise
    """
    # Terminate existing processes
    terminated_pids = ProcessManager.terminate_processes_by_name('python.exe', 'Benchmark_Portfolio_Terminal.py')
    logger.info(f"Terminated {len(terminated_pids)} Benchmark Portfolio Terminal processes")
    
    # Start new process
    if ProcessManager.start_process(config.benchmark_portfolio_terminal_path):
        logger.info("Started new Benchmark Portfolio Terminal")
        await send_rate_limited_warning(config.chat_id, "Benchmark Portfolio Terminal has been restarted.")
        return True
    else:
        logger.error(f"Failed to start Benchmark Portfolio Terminal")
        await send_rate_limited_warning(config.chat_id, f"Failed to restart Benchmark Portfolio Terminal: Could not start process")
        return False

async def periodic_check(interval: int) -> None:
    """Periodically checks all monitoring functions.
    
    Args:
        interval: Check interval in seconds
    """
    logger.info(f"Starting periodic checks with {interval}s interval")
    check_functions = [
        ("EMS Log", check_ems_log),
        ("Quote Catcher", check_and_restart_quote_catcher),
        ("Ping File", check_ping_file),
        ("Pending Orders", check_pending_orders),
        ("Benchmark Portfolio Terminal", check_benchmark_portfolio_terminal),
    ]
    
    while True:
        try:
            for check_name, check_func in check_functions:
                try:
                    result = await check_func()
                    if result:
                        logger.debug(f"{check_name} check: PASSED")
                    else:
                        logger.warning(f"{check_name} check: FAILED")
                except Exception as e:
                    logger.error(f"Error in {check_name} check: {e}")
            
            await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"Critical error during periodic check: {e}")
            await asyncio.sleep(interval)  # Continue checking even after errors

async def main() -> None:
    """Main function that starts the bot and periodic checking."""
    periodic_check_task = None
    try:
        set_terminal_title("SonixenBot")
        logger.info("Starting SonixenBot...")
        
        # Terminate any existing QuoteCatcher processes
        logger.info("Cleaning up existing QuoteCatcher processes...")
        terminated_pids = terminate_quote_catcher_processes()
        if terminated_pids:
            logger.info(f"Terminated {len(terminated_pids)} existing QuoteCatcher processes")
        
        # Start Telegram client
        await client.start(bot_token=config.bot_token)
        logger.info('SonixenBot is up and running')
        
        # Start the periodic check task
        check_interval = int(os.getenv('CHECK_INTERVAL_SECONDS', '60'))  # Default 1 minute
        periodic_check_task = asyncio.create_task(periodic_check(check_interval))
        logger.info(f"Started periodic checks with {check_interval}s interval")
        
        # Run the client until disconnected
        await client.run_until_disconnected()
        
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except SystemExit as e:
        logger.warning(f"SystemExit received: {e}")
    except Exception as e:
        logger.error(f"Critical error in main function: {e}")
        raise
    finally:
        # Cleanup
        logger.info("Shutting down SonixenBot...")
        if periodic_check_task and not periodic_check_task.done():
            periodic_check_task.cancel()
            try:
                await periodic_check_task
            except asyncio.CancelledError:
                logger.debug("Periodic check task cancelled successfully")
        
        if client.is_connected():
            await client.disconnect()
            logger.info("Telegram client disconnected")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("SonixenBot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
