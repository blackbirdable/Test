import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import matplotlib.dates as mdates
import os
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import time
import traceback
import glob
import re
import argparse
import tkinter as tk
from tkinter import ttk, messagebox

# ===========================================================================
# USER CONFIGURATION - Easily modify these variables to change settings
# ===========================================================================
# Base directory for output - strategy-specific directories will be created within this
BASE_OUTPUT_DIR = os.getcwd()

# Parameter configurations for different strategies
BTD_SHORT_CONFIG = {
    'name': 'BTD Short Term',
    'candle_minutes_range': [25, 70, 1],         # 25 to 70 minutes in steps of 1
    'dip_threshold_range': [-0.02, -0.05, -0.01], # -2% to -7% in steps of -1%
    'exit_profit_range': [0.02, 0.07, 0.01],      # 2% to 7% in steps of 1%
    'stop_loss_range': [-0.02, -0.07, -0.01],     # -2% to -7% in steps of -1%
    'strategy_type': 'BTD'
}

BTD_LONG_CONFIG = {
    'name': 'BTD Long Term',
    'candle_minutes_range': [150, 230, 5],        # 150 to 200 minutes in steps of 1
    'dip_threshold_range': [-0.02, -0.06, -0.01], # -2% to -7% in steps of -1%
    'exit_profit_range': [0.02, 0.06, 0.01],      # 2% to 7% in steps of 1%
    'stop_loss_range': [-0.02, -0.06, -0.01],     # -2% to -7% in steps of -1%
    'strategy_type': 'BTD'
}

STR_SHORT_CONFIG = {
    'name': 'STR Short Term',
    'candle_minutes_range': [25, 70, 1],          # 30 to 70 minutes in steps of 1
    'rip_threshold_range': [0.02, 0.05, 0.01],    # 2% to 5% in steps of 1%
    'exit_profit_range': [0.02, 0.07, 0.01],      # 2% to 7% in steps of 1%
    'stop_loss_range': [-0.02, -0.07, -0.01],     # -2% to -7% in steps of -1%
    'strategy_type': 'STR'
}

STR_LONG_CONFIG = {
    'name': 'STR Long Term',
    'candle_minutes_range': [150, 250, 5],        # 150 to 250 minutes in steps of 5
    'rip_threshold_range': [0.02, 0.05, 0.01],    # 2% to 5% in steps of 1%
    'exit_profit_range': [0.02, 0.07, 0.01],      # 2% to 7% in steps of 1%
    'stop_loss_range': [-0.02, -0.07, -0.01],     # -2% to -7% in steps of -1%
    'strategy_type': 'STR'
}

# Minimum PnL threshold for selecting results (in percent)
MIN_PNL_THRESHOLD = 5.0  # Only consider combinations with at least 5% total return

# Starting number of days to use for initial backtest
INITIAL_DAYS = 20

# CSV data file path - make sure this exists in the current directory
CSV_PATH = 'HistoricalData.csv'

# Use all available CPU cores
total_cpu_cores = multiprocessing.cpu_count()
num_processes = total_cpu_cores
print(f"Using {num_processes} processes for parallel execution (all {total_cpu_cores} available cores)")


def get_latest_processed_date(output_dir):
    """
    Check the output directory for existing metric files and find the latest end date that was processed.
    Returns the latest end date found, or None if no files exist.
    """
    if not os.path.exists(output_dir):
        print(f"Output directory {output_dir} does not exist. Starting from the beginning.")
        return None
    
    # Find all metric files in the output directory
    metric_files = glob.glob(os.path.join(output_dir, "metrics_*.csv"))
    
    if not metric_files:
        print("No existing metric files found. Starting from the beginning.")
        return None
    
    latest_end_date = None
    latest_end_date_str = ""
    
    # Pattern to extract date range from filename: metrics_*_YYYYMMDD_to_YYYYMMDD_*.csv
    date_pattern = r'_(\d{8})_to_(\d{8})_'
    
    for file_path in metric_files:
        filename = os.path.basename(file_path)
        match = re.search(date_pattern, filename)
        
        if match:
            start_date_str = match.group(1)
            end_date_str = match.group(2)
            
            # Convert to datetime for comparison
            try:
                end_date = datetime.strptime(end_date_str, '%Y%m%d').date()
                
                if latest_end_date is None or end_date > latest_end_date:
                    latest_end_date = end_date
                    latest_end_date_str = end_date_str
            except ValueError:
                print(f"Warning: Could not parse date from filename {filename}")
                continue
    
    if latest_end_date:
        print(f"Found existing results. Latest processed end date: {latest_end_date} ({latest_end_date_str})")
        return latest_end_date
    else:
        print("No valid date patterns found in existing files. Starting from the beginning.")
        return None


def get_trading_data(csv_path, start_date=None, end_date=None):
    """
    Extract trading data from a CSV file with optional date range filtering.
    """
    # Read the CSV file
    df = pd.read_csv(csv_path)
    
    # Convert Timestamp to datetime
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    
    # Filter data based on date range if provided
    if start_date is not None:
        # Convert to datetime if string is provided
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date)
        
        # Check if the Timestamp column has timezone info
        has_tz = df['Timestamp'].dt.tz is not None
        
        if has_tz and start_date.tzinfo is None:
            # If DataFrame has timezone but start_date doesn't, localize start_date to match
            start_date = pd.Timestamp(start_date).tz_localize(df['Timestamp'].dt.tz)
        elif not has_tz and start_date.tzinfo is not None:
            # If DataFrame doesn't have timezone but start_date does, convert DataFrame or make start_date naive
            start_date = pd.Timestamp(start_date).tz_localize(None)
            
        df = df[df['Timestamp'] >= start_date]
        
    if end_date is not None:
        # Convert to datetime if string is provided
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date)
            
        # Check if the Timestamp column has timezone info
        has_tz = df['Timestamp'].dt.tz is not None
        
        if has_tz and end_date.tzinfo is None:
            # If DataFrame has timezone but end_date doesn't, localize end_date to match
            end_date = pd.Timestamp(end_date).tz_localize(df['Timestamp'].dt.tz)
        elif not has_tz and end_date.tzinfo is not None:
            # If DataFrame doesn't have timezone but end_date does, make end_date naive
            end_date = pd.Timestamp(end_date).tz_localize(None)
            
        df = df[df['Timestamp'] <= end_date]
    
    # Sort by timestamp to ensure data is in chronological order
    df = df.sort_values('Timestamp')
    
    # Add date column for easier grouping by day
    df['Date'] = df['Timestamp'].dt.date
    
    return df


def run_btd_backtest(df, candle_minutes, dip_threshold, exit_profit, stop_loss):
    """
    Run the BTD strategy backtest with given parameters
    """
    # Group data into candlesticks
    df['TimeGroup'] = df['Timestamp'].dt.floor(f'{candle_minutes}min')

    # Prepare aggregation dictionary for OHLC
    agg_dict = {
        'WeightedSharePrice': ['first', 'max', 'min', 'last']
    }

    # Aggregate data into OHLC format
    candles = df.groupby('TimeGroup').agg(agg_dict)
    candles.columns = ['Open', 'High', 'Low', 'Close']  # Rename columns
    candles = candles.reset_index()
    
    # Calculate the percentage drop from high to low within each candle
    candles['IntraCandleDrop'] = (candles['Low'] - candles['High']) / candles['High']
    
    # Identify dips where the intra-candle drop is below our threshold
    candles['Dip'] = candles['IntraCandleDrop'] < dip_threshold
    
    # Initialize columns for signals and returns
    candles['BuySignal'] = False
    candles['SellSignal'] = False
    candles['StopLoss'] = False
    candles['ExitType'] = ''
    candles['BuyPrice'] = 0.0
    candles['Return'] = 0.0
    candles['TradeID'] = 0
    
    # Initialize variables for tracking positions
    in_position = False
    buy_price = 0
    buy_time = None
    buy_index = 0
    trade_id = 0
    trades = []  # List to store trade history
    
    # Iterate through the candlesticks to generate buy and sell signals
    for i in range(len(candles)):
        if not in_position and candles.loc[i, 'Dip']:
            # Calculate entry price at the moment the dip happened (when price dropped by threshold from high)
            dip_high = candles.loc[i, 'High']
            # Entry would happen when price dropped by dip_threshold from high
            buy_price = dip_high * (1 + dip_threshold)
            buy_time = candles.loc[i, 'TimeGroup']
            buy_index = i
            trade_id += 1
            candles.loc[i, 'BuySignal'] = True
            candles.loc[i, 'BuyPrice'] = buy_price
            candles.loc[i, 'TradeID'] = trade_id
            in_position = True
        elif in_position:
            # Track this candle as part of the current trade
            candles.loc[i, 'TradeID'] = trade_id
            
            # Check if profit target was hit (using High to check if we hit target during the candle)
            profit_target_hit = candles.loc[i, 'High'] >= buy_price * (1 + exit_profit)
            
            # Check if stop loss was hit (using Low to check if we hit stop loss during the candle)
            stop_loss_hit = candles.loc[i, 'Low'] <= buy_price * (1 + stop_loss)
            
            # Exit logic - prioritize stop loss if both are hit in the same candle
            if stop_loss_hit:
                in_position = False
                sell_time = candles.loc[i, 'TimeGroup']
                # Assume we sell at the stop loss price
                sell_price = buy_price * (1 + stop_loss)
                candles.loc[i, 'SellSignal'] = True
                candles.loc[i, 'StopLoss'] = True
                candles.loc[i, 'ExitType'] = 'Stop Loss'
                candles.loc[i, 'Return'] = stop_loss  # The return is the stop loss percentage
                
                # Calculate time in this trade
                time_in_trade_seconds = (sell_time - buy_time).total_seconds()
                
                # Calculate max drawdown for this trade
                trade_candles = candles[(candles['TradeID'] == trade_id) & 
                                      (candles.index >= buy_index) & 
                                      (candles.index <= i)]
                
                # Calculate the lowest price reached during the trade
                min_price = trade_candles['Low'].min()
                max_drawdown_pct = (min_price - buy_price) / buy_price
                
                # Record the trade
                trades.append({
                    'TradeID': trade_id,
                    'Entry Date': buy_time,
                    'Exit Date': sell_time,
                    'Duration (minutes)': time_in_trade_seconds / 60,
                    'Entry Price': buy_price,
                    'Exit Price': sell_price,
                    'Exit Type': 'Stop Loss',
                    'Return (%)': stop_loss * 100,
                    'Profit': sell_price - buy_price,
                    'Max Drawdown (%)': max_drawdown_pct * 100
                })
                
            elif profit_target_hit:
                in_position = False
                sell_time = candles.loc[i, 'TimeGroup']
                # Assume we sell at the target price
                sell_price = buy_price * (1 + exit_profit)
                candles.loc[i, 'SellSignal'] = True
                candles.loc[i, 'ExitType'] = 'Profit Target'
                candles.loc[i, 'Return'] = exit_profit  # We know we got exactly our exit profit
                
                # Calculate time in this trade
                time_in_trade_seconds = (sell_time - buy_time).total_seconds()
                
                # Calculate max drawdown for this trade
                trade_candles = candles[(candles['TradeID'] == trade_id) & 
                                      (candles.index >= buy_index) & 
                                      (candles.index <= i)]
                
                # Calculate the lowest price reached during the trade
                min_price = trade_candles['Low'].min()
                max_drawdown_pct = (min_price - buy_price) / buy_price
                
                # Record the trade
                trades.append({
                    'TradeID': trade_id,
                    'Entry Date': buy_time,
                    'Exit Date': sell_time,
                    'Duration (minutes)': time_in_trade_seconds / 60,
                    'Entry Price': buy_price,
                    'Exit Price': sell_price,
                    'Exit Type': 'Profit Target',
                    'Return (%)': exit_profit * 100,
                    'Profit': sell_price - buy_price,
                    'Max Drawdown (%)': max_drawdown_pct * 100
                })
    
    # Handle the case where we're still in a position at the end of the dataset
    if in_position:
        # Calculate the return for the final position using the last price
        final_price = candles['Close'].iloc[-1]
        last_idx = candles.index[-1]
        last_time = candles['TimeGroup'].iloc[-1]
        candles.loc[last_idx, 'SellSignal'] = True
        candles.loc[last_idx, 'ExitType'] = 'End of Data'
        candles.loc[last_idx, 'Return'] = (final_price - buy_price) / buy_price
        
        # Calculate time in the final trade
        time_in_final_trade_seconds = (last_time - buy_time).total_seconds()
        
        # Calculate max drawdown for this trade
        trade_candles = candles[(candles['TradeID'] == trade_id) & 
                              (candles.index >= buy_index)]
        
        # Calculate the lowest price reached during the trade
        min_price = trade_candles['Low'].min()
        max_drawdown_pct = (min_price - buy_price) / buy_price
        
        # Record the final trade
        trades.append({
            'TradeID': trade_id,
            'Entry Date': buy_time,
            'Exit Date': last_time,
            'Duration (minutes)': time_in_final_trade_seconds / 60,
            'Entry Price': buy_price,
            'Exit Price': final_price,
            'Exit Type': 'End of Data',
            'Return (%)': ((final_price - buy_price) / buy_price) * 100,
            'Profit': final_price - buy_price,
            'Max Drawdown (%)': max_drawdown_pct * 100
        })
    
    # Calculate cumulative returns
    candles['CumulativeReturn'] = candles['Return'].cumsum()

    # Calculate drawdowns
    candles['PeakValue'] = candles['CumulativeReturn'].cummax()
    candles['Drawdown'] = candles['CumulativeReturn'] - candles['PeakValue']
    
    # Create trades DataFrame
    trades_df = pd.DataFrame(trades)
    
    # Calculate key metrics
    final_return = candles['CumulativeReturn'].iloc[-1] if not candles.empty else 0
    max_drawdown = candles['Drawdown'].min() if not candles.empty else 0
    
    # Count different exit types
    num_trades = len(trades_df)
    profit_target_trades = sum(1 for trade in trades if trade['Exit Type'] == 'Profit Target') if trades else 0
    stop_loss_trades = sum(1 for trade in trades if trade['Exit Type'] == 'Stop Loss') if trades else 0
    other_exit_trades = num_trades - profit_target_trades - stop_loss_trades
    
    # Calculate win rate
    win_rate = profit_target_trades / num_trades * 100 if num_trades > 0 else 0
    
    # Calculate time metrics
    total_time_in_market_minutes = sum(trade['Duration (minutes)'] for trade in trades) if trades else 0
    total_time_in_market_hours = total_time_in_market_minutes / 60
    
    # Calculate total dataset time
    if not candles.empty:
        total_dataset_seconds = (candles['TimeGroup'].iloc[-1] - candles['TimeGroup'].iloc[0]).total_seconds()
        total_dataset_hours = total_dataset_seconds / 3600
    else:
        total_dataset_hours = 0
    
    # Calculate percentage of time in market
    time_in_market_percentage = (total_time_in_market_hours / total_dataset_hours) * 100 if total_dataset_hours > 0 else 0
    
    # Calculate average trade metrics
    avg_trade_duration = total_time_in_market_minutes / num_trades if num_trades > 0 else 0
    avg_max_drawdown = trades_df['Max Drawdown (%)'].mean() if not trades_df.empty else 0
    worst_drawdown = trades_df['Max Drawdown (%)'].min() if not trades_df.empty else 0
    
    # Store key metrics in results dictionary
    results = {
        'parameters': {
            'candle_minutes': candle_minutes,
            'dip_threshold': dip_threshold,
            'exit_profit': exit_profit,
            'stop_loss': stop_loss
        },
        'candles': candles,
        'trades': trades_df,
        'metrics': {
            'final_return': final_return * 100,  # Convert to percentage
            'num_trades': num_trades,
            'profit_target_trades': profit_target_trades,
            'stop_loss_trades': stop_loss_trades,
            'other_exit_trades': other_exit_trades,
            'win_rate': win_rate,
            'max_drawdown': max_drawdown * 100,  # Convert to percentage
            'time_in_market_hours': total_time_in_market_hours,
            'time_in_market_percentage': time_in_market_percentage,
            'avg_trade_duration': avg_trade_duration,
            'avg_max_drawdown': avg_max_drawdown,
            'worst_drawdown': worst_drawdown
        }
    }
    
    return results


def save_results(results, output_dir, start_date, end_date, strategy_type):
    """
    Save backtest results to CSV files
    """
    # Create parameter string for filenames with PnL included
    params = results['parameters']
    final_return = results['metrics']['final_return']
    
    if strategy_type == 'BTD':
        param_string = f"{final_return:.0f}%PNL,{abs(params['stop_loss']*100):.0f}%SL,{params['candle_minutes']}Candle,{abs(params['dip_threshold']*100):.0f}%DIP,{params['exit_profit']*100:.0f}%TP"
    else:  # STR
        param_string = f"{final_return:.0f}%PNL,{abs(params['stop_loss']*100):.0f}%SL,{params['candle_minutes']}Candle,{abs(params['rip_threshold']*100):.0f}%RIP,{params['exit_profit']*100:.0f}%TP"
    
    # Format dates for the filename (YYYYMMDD format)
    start_date_str = start_date.strftime("%Y%m%d") if hasattr(start_date, 'strftime') else str(start_date).replace('-', '')
    end_date_str = end_date.strftime("%Y%m%d") if hasattr(end_date, 'strftime') else str(end_date).replace('-', '')
    date_range = f"{start_date_str}_to_{end_date_str}"
    
    # Create timestamp for files
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Save metrics to CSV
    metrics_df = pd.DataFrame([results['metrics']])
    metrics_df['param_string'] = param_string
    metrics_df['candle_minutes'] = params['candle_minutes']
    
    if strategy_type == 'BTD':
        metrics_df['dip_threshold'] = params['dip_threshold'] * 100  # Convert to percentage
    else:  # STR
        metrics_df['rip_threshold'] = params['rip_threshold'] * 100  # Convert to percentage
    
    metrics_df['exit_profit'] = params['exit_profit'] * 100  # Convert to percentage
    metrics_df['stop_loss'] = params['stop_loss'] * 100  # Convert to percentage
    metrics_df['start_date'] = str(start_date)
    metrics_df['end_date'] = str(end_date)
    
    # Updated filename format without strategy_type prefix
    metrics_filename = os.path.join(output_dir, f"metrics_{param_string}_{date_range}_{current_time}.csv")
    metrics_df.to_csv(metrics_filename, index=False)
    
    return metrics_filename, param_string, final_return


def run_parameter_test(args):
    """
    Function to run a single parameter combination - for parallel processing
    """
    df, candle_minutes, threshold, exit_profit, stop_loss, strategy_type = args
    
    # Run backtest with current parameters based on strategy type
    if strategy_type == 'BTD':
        results = run_btd_backtest(df, int(candle_minutes), threshold, exit_profit, stop_loss)
    else:  # STR
        results = run_str_backtest(df, int(candle_minutes), threshold, exit_profit, stop_loss)
    
    # Extract key parameters and metrics
    param_results = {
        'candle_minutes': candle_minutes,
        'threshold': threshold,
        'exit_profit': exit_profit,
        'stop_loss': stop_loss,
        'strategy_type': strategy_type,
        'final_return': results['metrics']['final_return'],
        'num_trades': results['metrics']['num_trades'],
        'win_rate': results['metrics']['win_rate'],
        'max_drawdown': results['metrics']['max_drawdown'],
        'time_in_market_percentage': results['metrics']['time_in_market_percentage'],
        'results': results  # Include full results for saving later
    }
    
    return param_results


class ParameterSearchMenu:
    def __init__(self, root):
        self.root = root
        self.root.title("Parameter Grid Search Selector")
        self.root.geometry("800x600")
        self.root.resizable(False, False)
        
        # Center the window on screen
        self.root.update_idletasks()
        width = self.root.winfo_width()
        height = self.root.winfo_height()
        x = (self.root.winfo_screenwidth() // 2) - (width // 2)
        y = (self.root.winfo_screenheight() // 2) - (height // 2)
        self.root.geometry(f'{width}x{height}+{x}+{y}')
        
        self.selected_config = None
        
        # Main frame with padding
        main_frame = ttk.Frame(root, padding="30")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Title
        title_label = ttk.Label(main_frame, text="Parameter Grid Search Configuration", 
                               font=("Arial", 16, "bold"))
        title_label.pack(anchor="w", pady=(0, 20))
        
        # Subtitle
        subtitle_label = ttk.Label(main_frame, text="Select a parameter search configuration to optimize trading strategies", 
                                  font=("Arial", 10))
        subtitle_label.pack(anchor="w", pady=(0, 30))
        
        # Strategy selection frame
        strategy_frame = ttk.LabelFrame(main_frame, text="Available Parameter Search Configurations", padding=(20, 15))
        strategy_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 20))
        
        # BTD Section
        btd_frame = ttk.LabelFrame(strategy_frame, text="Buy The Dip (BTD) Parameter Search", padding=(15, 10))
        btd_frame.pack(fill="x", pady=(0, 15))
        
        # BTD Short Term
        btd_short_frame = ttk.Frame(btd_frame)
        btd_short_frame.pack(fill="x", pady=(0, 10))
        
        ttk.Button(btd_short_frame, text="BTD Short Term", 
                  command=lambda: self.select_config(BTD_SHORT_CONFIG),
                  width=20).pack(side=tk.LEFT)
        
        btd_short_desc = f"Candles: {BTD_SHORT_CONFIG['candle_minutes_range'][0]}-{BTD_SHORT_CONFIG['candle_minutes_range'][1]} min, "
        btd_short_desc += f"Dip: {abs(BTD_SHORT_CONFIG['dip_threshold_range'][0]*100):.0f}-{abs(BTD_SHORT_CONFIG['dip_threshold_range'][1]*100):.0f}%, "
        btd_short_desc += f"Profit: {BTD_SHORT_CONFIG['exit_profit_range'][0]*100:.0f}-{BTD_SHORT_CONFIG['exit_profit_range'][1]*100:.0f}%, "
        btd_short_desc += f"Stop: {abs(BTD_SHORT_CONFIG['stop_loss_range'][0]*100):.0f}-{abs(BTD_SHORT_CONFIG['stop_loss_range'][1]*100):.0f}%"
        
        ttk.Label(btd_short_frame, text=btd_short_desc,
                 font=("Arial", 8), wraplength=500).pack(side=tk.LEFT, padx=(15, 0))
        
        # BTD Long Term
        btd_long_frame = ttk.Frame(btd_frame)
        btd_long_frame.pack(fill="x")
        
        ttk.Button(btd_long_frame, text="BTD Long Term", 
                  command=lambda: self.select_config(BTD_LONG_CONFIG),
                  width=20).pack(side=tk.LEFT)
        
        btd_long_desc = f"Candles: {BTD_LONG_CONFIG['candle_minutes_range'][0]}-{BTD_LONG_CONFIG['candle_minutes_range'][1]} min, "
        btd_long_desc += f"Dip: {abs(BTD_LONG_CONFIG['dip_threshold_range'][0]*100):.0f}-{abs(BTD_LONG_CONFIG['dip_threshold_range'][1]*100):.0f}%, "
        btd_long_desc += f"Profit: {BTD_LONG_CONFIG['exit_profit_range'][0]*100:.0f}-{BTD_LONG_CONFIG['exit_profit_range'][1]*100:.0f}%, "
        btd_long_desc += f"Stop: {abs(BTD_LONG_CONFIG['stop_loss_range'][0]*100):.0f}-{abs(BTD_LONG_CONFIG['stop_loss_range'][1]*100):.0f}%"
        
        ttk.Label(btd_long_frame, text=btd_long_desc,
                 font=("Arial", 8), wraplength=500).pack(side=tk.LEFT, padx=(15, 0))
        
        # STR Section
        str_frame = ttk.LabelFrame(strategy_frame, text="Sell The Rip (STR) Parameter Search", padding=(15, 10))
        str_frame.pack(fill="x", pady=(0, 15))
        
        # STR Short Term
        str_short_frame = ttk.Frame(str_frame)
        str_short_frame.pack(fill="x", pady=(0, 10))
        
        ttk.Button(str_short_frame, text="STR Short Term", 
                  command=lambda: self.select_config(STR_SHORT_CONFIG),
                  width=20).pack(side=tk.LEFT)
        
        str_short_desc = f"Candles: {STR_SHORT_CONFIG['candle_minutes_range'][0]}-{STR_SHORT_CONFIG['candle_minutes_range'][1]} min, "
        str_short_desc += f"Rip: {STR_SHORT_CONFIG['rip_threshold_range'][0]*100:.0f}-{STR_SHORT_CONFIG['rip_threshold_range'][1]*100:.0f}%, "
        str_short_desc += f"Profit: {STR_SHORT_CONFIG['exit_profit_range'][0]*100:.0f}-{STR_SHORT_CONFIG['exit_profit_range'][1]*100:.0f}%, "
        str_short_desc += f"Stop: {abs(STR_SHORT_CONFIG['stop_loss_range'][0]*100):.0f}-{abs(STR_SHORT_CONFIG['stop_loss_range'][1]*100):.0f}%"
        
        ttk.Label(str_short_frame, text=str_short_desc,
                 font=("Arial", 8), wraplength=500).pack(side=tk.LEFT, padx=(15, 0))
        
        # STR Long Term
        str_long_frame = ttk.Frame(str_frame)
        str_long_frame.pack(fill="x")
        
        ttk.Button(str_long_frame, text="STR Long Term", 
                  command=lambda: self.select_config(STR_LONG_CONFIG),
                  width=20).pack(side=tk.LEFT)
        
        str_long_desc = f"Candles: {STR_LONG_CONFIG['candle_minutes_range'][0]}-{STR_LONG_CONFIG['candle_minutes_range'][1]} min (step {STR_LONG_CONFIG['candle_minutes_range'][2]}), "
        str_long_desc += f"Rip: {STR_LONG_CONFIG['rip_threshold_range'][0]*100:.0f}-{STR_LONG_CONFIG['rip_threshold_range'][1]*100:.0f}%, "
        str_long_desc += f"Profit: {STR_LONG_CONFIG['exit_profit_range'][0]*100:.0f}-{STR_LONG_CONFIG['exit_profit_range'][1]*100:.0f}%, "
        str_long_desc += f"Stop: {abs(STR_LONG_CONFIG['stop_loss_range'][0]*100):.0f}-{abs(STR_LONG_CONFIG['stop_loss_range'][1]*100):.0f}%"
        
        ttk.Label(str_long_frame, text=str_long_desc,
                 font=("Arial", 8), wraplength=500).pack(side=tk.LEFT, padx=(15, 0))
        
        # Configuration Info
        info_frame = ttk.LabelFrame(main_frame, text="Configuration Information", padding=(20, 15))
        info_frame.pack(fill="x", pady=(0, 20))
        
        info_text = f"• Minimum PnL Threshold: {MIN_PNL_THRESHOLD}%\n"
        info_text += f"• Initial Days for Testing: {INITIAL_DAYS}\n"
        info_text += f"• Data Source: {CSV_PATH}\n"
        info_text += f"• CPU Cores Used: {num_processes}\n"
        info_text += f"• Output Base Directory: {BASE_OUTPUT_DIR}"
        
        ttk.Label(info_frame, text=info_text, font=("Arial", 9)).pack(anchor="w")
        
        # Bottom buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill="x", pady=(10, 0))
        
        # Exit button (left side)
        ttk.Button(button_frame, text="Exit", command=self.root.quit, 
                  width=12).pack(side=tk.LEFT)
        
        # Status label (right side)
        self.status_label = ttk.Label(button_frame, text="Select a configuration to begin parameter search", 
                                     font=("Arial", 9), foreground="gray")
        self.status_label.pack(side=tk.RIGHT)
    
    def select_config(self, config):
        """Handle configuration selection and close menu"""
        self.selected_config = config
        self.status_label.config(text=f"Starting {config['name']} parameter search...", foreground="green")
        self.root.update()
        
        # Small delay to show the status message
        self.root.after(1000, self.root.destroy)


def show_parameter_search_menu():
    """Show parameter search menu and return selected configuration"""
    root = tk.Tk()
    menu = ParameterSearchMenu(root)
    root.mainloop()
    
    return menu.selected_config


def run_parameter_search(config):
    """
    Main function that runs the cumulative testing with increasing date ranges using the selected configuration
    """
    print(f"\nSelected Configuration: {config['name']}")
    print(f"Strategy Type: {config['strategy_type']}")
    
    # Create strategy-specific output directory with proper naming
    # Map config names to the desired directory names
    if config['name'] == 'BTD Short Term':
        strategy_output_dir = os.path.join(BASE_OUTPUT_DIR, "BTD_ShortTerm_test_results")
    elif config['name'] == 'BTD Long Term':
        strategy_output_dir = os.path.join(BASE_OUTPUT_DIR, "BTD_LongTerm_test_results")
    elif config['name'] == 'STR Short Term':
        strategy_output_dir = os.path.join(BASE_OUTPUT_DIR, "STR_ShortTerm_test_results")
    elif config['name'] == 'STR Long Term':
        strategy_output_dir = os.path.join(BASE_OUTPUT_DIR, "STR_LongTerm_test_results")
    else:
        # Fallback to original logic
        strategy_output_dir = os.path.join(BASE_OUTPUT_DIR, f"{config['strategy_type']}_{config['name'].replace(' ', '_')}")
    
    # Check if output directory exists, create if not
    if not os.path.exists(strategy_output_dir):
        os.makedirs(strategy_output_dir)
        print(f"Created output directory: {strategy_output_dir}")
    
    # Check for existing results and determine where to resume
    latest_processed_date = get_latest_processed_date(strategy_output_dir)
    
    # Load trading data
    print(f"Loading trading data from {CSV_PATH}...")
    df = get_trading_data(CSV_PATH)
    
    if df.empty:
        print("Error: No data found in the CSV file.")
        return
    
    # Get unique dates in the dataset
    unique_dates = sorted(df['Date'].unique())
    num_dates = len(unique_dates)
    
    print(f"Loaded {len(df)} data points spanning from {df['Timestamp'].min()} to {df['Timestamp'].max()}")
    print(f"Found {num_dates} unique trading days")
    
    if num_dates < INITIAL_DAYS:
        print(f"Error: Not enough data. Need at least {INITIAL_DAYS} days, but only found {num_dates}.")
        return
    
    # Determine starting point based on existing results
    start_date = unique_dates[0]
    
    if latest_processed_date:
        # Find the index of the latest processed date
        try:
            latest_index = unique_dates.index(latest_processed_date)
            start_end_idx = latest_index + 1  # Start from the next date
            
            if start_end_idx >= num_dates:
                print(f"All dates have been processed. Latest date in data: {unique_dates[-1]}")
                print("No new dates to process.")
                return
            
            print(f"Resuming from date index {start_end_idx} (date: {unique_dates[start_end_idx]})")
            
        except ValueError:
            print(f"Warning: Latest processed date {latest_processed_date} not found in dataset.")
            print("Starting from the beginning.")
            start_end_idx = INITIAL_DAYS
    else:
        start_end_idx = INITIAL_DAYS
        print(f"Starting fresh from day {INITIAL_DAYS}")
    
    # Generate parameter ranges based on configuration
    candle_minutes_values = list(range(
        config['candle_minutes_range'][0], 
        config['candle_minutes_range'][1] + 1, 
        config['candle_minutes_range'][2]))
    
    # Generate threshold values (dip for BTD, rip for STR)
    if config['strategy_type'] == 'BTD':
        threshold_range = config['dip_threshold_range']
        threshold_name = 'dip'
    else:
        threshold_range = config['rip_threshold_range']
        threshold_name = 'rip'
    
    threshold_values = []
    if threshold_range[0] < threshold_range[1]:  # Positive range (STR)
        value = threshold_range[0]
        while value <= threshold_range[1]:
            threshold_values.append(value)
            value += threshold_range[2]
    else:  # Negative range (BTD)
        value = threshold_range[0]
        while value >= threshold_range[1]:
            threshold_values.append(value)
            value += threshold_range[2]
    
    exit_profit_values = []
    value = config['exit_profit_range'][0]
    while value <= config['exit_profit_range'][1]:
        exit_profit_values.append(value)
        value += config['exit_profit_range'][2]
    
    stop_loss_values = []
    value = config['stop_loss_range'][0]
    while value >= config['stop_loss_range'][1]:
        stop_loss_values.append(value)
        value += config['stop_loss_range'][2]
    
    # Print parameter grid size
    total_combinations = (
        len(candle_minutes_values) * 
        len(threshold_values) * 
        len(exit_profit_values) * 
        len(stop_loss_values)
    )
    
    print(f"Parameter grid: "
          f"{len(candle_minutes_values)} candle periods × "
          f"{len(threshold_values)} {threshold_name} thresholds × "
          f"{len(exit_profit_values)} profit targets × "
          f"{len(stop_loss_values)} stop losses = "
          f"{total_combinations} combinations")
    
    # Process each cumulative date range starting from the determined point
    dates_to_process = num_dates - start_end_idx
    print(f"Will process {dates_to_process} date ranges")
    
    for end_idx in range(start_end_idx, num_dates):
        end_date = unique_dates[end_idx]
        
        print(f"\nProcessing cumulative range from {start_date} to {end_date} ({end_idx + 1}/{num_dates} days)")
        print(f"Progress: {end_idx - start_end_idx + 1}/{dates_to_process} remaining date ranges")
        
        # Get data for this date range
        date_range_df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]
        
        # Prepare parameter combinations for this date range
        param_combinations = []
        for cm in candle_minutes_values:
            for th in threshold_values:
                for ep in exit_profit_values:
                    for sl in stop_loss_values:
                        param_combinations.append((date_range_df, cm, th, ep, sl, config['strategy_type']))
        
        print(f"Testing {len(param_combinations)} parameter combinations on {len(date_range_df)} data points...")
        
        # Run parameter combinations in parallel
        results = []
        with ProcessPoolExecutor(max_workers=num_processes) as executor:
            for i, result in enumerate(executor.map(run_parameter_test, param_combinations)):
                results.append(result)
                # Print progress every 100 combinations
                if (i+1) % 100 == 0:
                    print(f"  Processed {i+1}/{len(param_combinations)} combinations ({(i+1)/len(param_combinations)*100:.1f}%)")
        
        # Filter for profitable combinations
        profitable_results = [r for r in results if r['final_return'] >= MIN_PNL_THRESHOLD]
        
        if not profitable_results:
            print(f"  No parameter combinations met the {MIN_PNL_THRESHOLD}% PnL threshold for this date range")
            continue
        
        print(f"  Found {len(profitable_results)} combinations with PnL >= {MIN_PNL_THRESHOLD}%")
        
        # Group profitable results by candle_minutes
        grouped_by_candle = {}
        for result in profitable_results:
            cm = result['candle_minutes']
            if cm not in grouped_by_candle:
                grouped_by_candle[cm] = []
            grouped_by_candle[cm].append(result)
        
        # For each candle_minutes group, select the highest PnL result
        # If tied, choose the one with the lowest (least negative) stop loss
        top_per_candle = []
        for cm, group in grouped_by_candle.items():
            # Sort by PnL (descending) then stop_loss (ascending - less negative is better)
            sorted_group = sorted(group, key=lambda x: (x['final_return'], x['stop_loss']), reverse=True)
            top_per_candle.append(sorted_group[0])
        
        # Sort by PnL and take top 5 (or fewer if not enough results)
        top_results = sorted(top_per_candle, key=lambda x: x['final_return'], reverse=True)[:5]
        
        print(f"  Top results after grouping by candle_minutes:")
        for idx, result in enumerate(top_results):
            if config['strategy_type'] == 'BTD':
                print(f"    {idx+1}. PnL: {result['final_return']:.2f}%, "
                      f"CM: {result['candle_minutes']}, "
                      f"DT: {result['threshold']*100:.1f}%, "
                      f"EP: {result['exit_profit']*100:.1f}%, "
                      f"SL: {result['stop_loss']*100:.1f}%")
            else:  # STR
                print(f"    {idx+1}. PnL: {result['final_return']:.2f}%, "
                      f"CM: {result['candle_minutes']}, "
                      f"RT: {result['threshold']*100:.1f}%, "
                      f"EP: {result['exit_profit']*100:.1f}%, "
                      f"SL: {result['stop_loss']*100:.1f}%")
        
        # Save the top results
        print(f"  Saving {len(top_results)} top results...")
        saved_files = []
        for result in top_results:
            filename, param_string, pnl = save_results(result['results'], strategy_output_dir, start_date, end_date, config['strategy_type'])
            saved_files.append((filename, param_string, pnl))
        
        print(f"  Saved files:")
        for filename, param_string, pnl in saved_files:
            print(f"    {os.path.basename(filename)} - PnL: {pnl:.2f}%")
    
    print(f"\nCompleted all cumulative date ranges for {config['name']}!")


def main():
    """Main function to run the application"""
    parser = argparse.ArgumentParser(description='Parameter Grid Search with Menu Selection')
    parser.add_argument('--menu', action='store_true', help='Show configuration selection menu')
    parser.add_argument('--config', choices=['BTD_SHORT', 'BTD_LONG', 'STR_SHORT', 'STR_LONG'], 
                       help='Directly specify configuration without menu')
    args = parser.parse_args()
    
    # If no arguments provided, show the menu by default
    if len(os.sys.argv) == 1:
        args.menu = True
    
    if args.menu:
        # Show menu and get selected configuration
        selected_config = show_parameter_search_menu()
        
        if selected_config:
            print(f"Selected configuration: {selected_config['name']}")
            run_parameter_search(selected_config)
        else:
            print("No configuration selected. Exiting.")
    elif args.config:
        # Direct configuration selection
        config_map = {
            'BTD_SHORT': BTD_SHORT_CONFIG,
            'BTD_LONG': BTD_LONG_CONFIG,
            'STR_SHORT': STR_SHORT_CONFIG,
            'STR_LONG': STR_LONG_CONFIG
        }
        
        selected_config = config_map[args.config]
        print(f"Running with configuration: {selected_config['name']}")
        run_parameter_search(selected_config)
    else:
        # Default to showing menu if no specific config is provided
        selected_config = show_parameter_search_menu()
        
        if selected_config:
            print(f"Selected configuration: {selected_config['name']}")
            run_parameter_search(selected_config)
        else:
            print("No configuration selected. Exiting.")


if __name__ == "__main__":
    try:
        start_time = time.time()
        main()
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"\nScript executed in {execution_time:.2f} seconds")
    except Exception as e:
        print(f"Error occurred: {e}")
        print(traceback.format_exc())


def run_str_backtest(df, candle_minutes, rip_threshold, exit_profit, stop_loss):
    """
    Run the STR strategy backtest with given parameters
    """
    # Group data into candlesticks
    df['TimeGroup'] = df['Timestamp'].dt.floor(f'{candle_minutes}min')

    # Prepare aggregation dictionary for OHLC
    agg_dict = {
        'WeightedSharePrice': ['first', 'max', 'min', 'last']
    }

    # Aggregate data into OHLC format
    candles = df.groupby('TimeGroup').agg(agg_dict)
    candles.columns = ['Open', 'High', 'Low', 'Close']  # Rename columns
    candles = candles.reset_index()
    
    # Calculate the percentage rise from low to high within each candle
    candles['IntraCandleRise'] = (candles['High'] - candles['Low']) / candles['Low']
    
    # Identify rips where the intra-candle rise is above our threshold
    candles['Rip'] = candles['IntraCandleRise'] > rip_threshold
    
    # Initialize columns for signals and returns
    candles['SellSignal'] = False
    candles['BuySignal'] = False
    candles['StopLoss'] = False
    candles['ExitType'] = ''
    candles['SellPrice'] = 0.0
    candles['Return'] = 0.0
    candles['TradeID'] = 0
    
    # Initialize variables for tracking positions
    in_position = False
    sell_price = 0
    sell_time = None
    sell_index = 0
    trade_id = 0
    trades = []  # List to store trade history
    
    # Iterate through the candlesticks to generate sell and buy signals
    for i in range(len(candles)):
        if not in_position and candles.loc[i, 'Rip']:
            # Calculate entry price at the moment the rip happened (when price increased by threshold from low)
            rip_low = candles.loc[i, 'Low']
            # Entry would happen when price increased by rip_threshold from low
            sell_price = rip_low * (1 + rip_threshold)
            sell_time = candles.loc[i, 'TimeGroup']
            sell_index = i
            trade_id += 1
            candles.loc[i, 'SellSignal'] = True
            candles.loc[i, 'SellPrice'] = sell_price
            candles.loc[i, 'TradeID'] = trade_id
            in_position = True
        elif in_position:
            # Track this candle as part of the current trade
            candles.loc[i, 'TradeID'] = trade_id
            
            # Check if profit target was hit (using Low to check if we hit target during the candle)
            profit_target_hit = candles.loc[i, 'Low'] <= sell_price * (1 - exit_profit)
            
            # Check if stop loss was hit (using High to check if we hit stop loss during the candle)
            stop_loss_hit = candles.loc[i, 'High'] >= sell_price * (1 - stop_loss)
            
            # Exit logic - prioritize stop loss if both are hit in the same candle
            if stop_loss_hit:
                in_position = False
                buy_time = candles.loc[i, 'TimeGroup']
                # Assume we buy to cover at the stop loss price
                buy_price = sell_price * (1 - stop_loss)
                candles.loc[i, 'BuySignal'] = True
                candles.loc[i, 'StopLoss'] = True
                candles.loc[i, 'ExitType'] = 'Stop Loss'
                candles.loc[i, 'Return'] = stop_loss  # The return is the stop loss percentage (negative)
                
                # Calculate time in this trade
                time_in_trade_seconds = (buy_time - sell_time).total_seconds()
                
                # Calculate max drawdown for this trade
                trade_candles = candles[(candles['TradeID'] == trade_id) & 
                                      (candles.index >= sell_index) & 
                                      (candles.index <= i)]
                
                # Calculate the highest price reached during the trade (worst for short position)
                max_price = trade_candles['High'].max()
                max_drawdown_pct = (max_price - sell_price) / sell_price
                
                # Record the trade
                trades.append({
                    'TradeID': trade_id,
                    'Entry Date': sell_time,
                    'Exit Date': buy_time,
                    'Duration (minutes)': time_in_trade_seconds / 60,
                    'Entry Price': sell_price,
                    'Exit Price': buy_price,
                    'Exit Type': 'Stop Loss',
                    'Return (%)': stop_loss * 100,  # Convert to percentage for display
                    'Profit': sell_price - buy_price,  # For short position, profit is sell_price - buy_price
                    'Max Drawdown (%)': max_drawdown_pct * 100
                })
                
            elif profit_target_hit:
                in_position = False
                buy_time = candles.loc[i, 'TimeGroup']
                # Assume we buy at the target price
                buy_price = sell_price * (1 - exit_profit)
                candles.loc[i, 'BuySignal'] = True
                candles.loc[i, 'ExitType'] = 'Profit Target'
                candles.loc[i, 'Return'] = exit_profit  # We know we got exactly our exit profit
                
                # Calculate time in this trade
                time_in_trade_seconds = (buy_time - sell_time).total_seconds()
                
                # Calculate max drawdown for this trade
                trade_candles = candles[(candles['TradeID'] == trade_id) & 
                                      (candles.index >= sell_index) & 
                                      (candles.index <= i)]
                
                # Calculate the highest price reached during the trade (worst for short position)
                max_price = trade_candles['High'].max()
                max_drawdown_pct = (max_price - sell_price) / sell_price
                
                # Record the trade
                trades.append({
                    'TradeID': trade_id,
                    'Entry Date': sell_time,
                    'Exit Date': buy_time,
                    'Duration (minutes)': time_in_trade_seconds / 60,
                    'Entry Price': sell_price,
                    'Exit Price': buy_price,
                    'Exit Type': 'Profit Target',
                    'Return (%)': exit_profit * 100,  # Convert to percentage for display
                    'Profit': sell_price - buy_price,  # For short position, profit is sell_price - buy_price
                    'Max Drawdown (%)': max_drawdown_pct * 100
                })
    
    # Handle the case where we're still in a position at the end of the dataset
    if in_position:
        # Calculate the return for the final position using the last price
        final_price = candles['Close'].iloc[-1]
        last_idx = candles.index[-1]
        last_time = candles['TimeGroup'].iloc[-1]
        candles.loc[last_idx, 'BuySignal'] = True
        candles.loc[last_idx, 'ExitType'] = 'End of Data'
        candles.loc[last_idx, 'Return'] = (sell_price - final_price) / sell_price  # For short positions
        
        # Calculate time in the final trade
        time_in_final_trade_seconds = (last_time - sell_time).total_seconds()
        
        # Calculate max drawdown for this trade
        trade_candles = candles[(candles['TradeID'] == trade_id) & 
                              (candles.index >= sell_index)]
        
        # Calculate the highest price reached during the trade (worst for short position)
        max_price = trade_candles['High'].max()
        max_drawdown_pct = (max_price - sell_price) / sell_price
        
        # Record the final trade
        trades.append({
            'TradeID': trade_id,
            'Entry Date': sell_time,
            'Exit Date': last_time,
            'Duration (minutes)': time_in_final_trade_seconds / 60,
            'Entry Price': sell_price,
            'Exit Price': final_price,
            'Exit Type': 'End of Data',
            'Return (%)': ((sell_price - final_price) / sell_price) * 100,  # Convert to percentage for display
            'Profit': sell_price - final_price,  # For short position, profit is sell_price - buy_price
            'Max Drawdown (%)': max_drawdown_pct * 100
        })
    
    # Calculate cumulative returns
    candles['CumulativeReturn'] = candles['Return'].cumsum()

    # Calculate drawdowns
    candles['PeakValue'] = candles['CumulativeReturn'].cummax()
    candles['Drawdown'] = candles['CumulativeReturn'] - candles['PeakValue']
    
    # Create trades DataFrame
    trades_df = pd.DataFrame(trades)
    
    # Calculate key metrics
    final_return = candles['CumulativeReturn'].iloc[-1] if not candles.empty else 0
    max_drawdown = candles['Drawdown'].min() if not candles.empty else 0
    
    # Count different exit types
    num_trades = len(trades_df)
    profit_target_trades = sum(1 for trade in trades if trade['Exit Type'] == 'Profit Target') if trades else 0
    stop_loss_trades = sum(1 for trade in trades if trade['Exit Type'] == 'Stop Loss') if trades else 0
    other_exit_trades = num_trades - profit_target_trades - stop_loss_trades
    
    # Calculate win rate
    win_rate = profit_target_trades / num_trades * 100 if num_trades > 0 else 0
    
    # Calculate time metrics
    total_time_in_market_minutes = sum(trade['Duration (minutes)'] for trade in trades) if trades else 0
    total_time_in_market_hours = total_time_in_market_minutes / 60
    
    # Calculate total dataset time
    if not candles.empty:
        total_dataset_seconds = (candles['TimeGroup'].iloc[-1] - candles['TimeGroup'].iloc[0]).total_seconds()
        total_dataset_hours = total_dataset_seconds / 3600
    else:
        total_dataset_hours = 0
    
    # Calculate percentage of time in market
    time_in_market_percentage = (total_time_in_market_hours / total_dataset_hours) * 100 if total_dataset_hours > 0 else 0
    
    # Calculate average trade metrics
    avg_trade_duration = total_time_in_market_minutes / num_trades if num_trades > 0 else 0
    avg_max_drawdown = trades_df['Max Drawdown (%)'].mean() if not trades_df.empty else 0
    worst_drawdown = trades_df['Max Drawdown (%)'].min() if not trades_df.empty else 0
    
    # Store key metrics in results dictionary
    results = {
        'parameters': {
            'candle_minutes': candle_minutes,
            'rip_threshold': rip_threshold,
            'exit_profit': exit_profit,
            'stop_loss': stop_loss
        },
        'candles': candles,
        'trades': trades_df,
        'metrics': {
            'final_return': final_return * 100,  # Convert to percentage
            'num_trades': num_trades,
            'profit_target_trades': profit_target_trades,
            'stop_loss_trades': stop_loss_trades,
            'other_exit_trades': other_exit_trades,
            'win_rate': win_rate,
            'max_drawdown': max_drawdown * 100,  # Convert to percentage
            'time_in_market_hours': total_time_in_market_hours,
            'time_in_market_percentage': time_in_market_percentage,
            'avg_trade_duration': avg_trade_duration,
            'avg_max_drawdown': avg_max_drawdown,
            'worst_drawdown': worst_drawdown
        }
    }
    
    return results
