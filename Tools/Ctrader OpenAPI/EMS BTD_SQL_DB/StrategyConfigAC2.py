# StrategyConfig.py
# Define a list of strategy configurations

# Credentials
CLIENT_ID = '11198_muJeK9dYaCwseaddKNqqXpDIDbdOgjuQxnB359f6AvNfy8O0iN'
CLIENT_SECRET = 'QXOiXKbeENK9LP1WmnvU4itS5FcrSTi935TlX8TTx0zqRyVmhg'
ACCESS_TOKEN = 'RUv31AAaamZkJ6JEj3u4rJkKHE2TtqjzkErYNZdj0cY'
ACCOUNT_ID = 43123394  # Demo account ID

strategies = [
    {
        'label': 'TEST',
        'symbol_id': 10026,  
        'symbol_precision': 2,
        'depth_quotes_file': 'C:/Users/Administrator/Desktop/Sonixen/Logs/BTC_Quotes.py',
        'signal_file': 'C:/Users/Administrator/Desktop/Sonixen/Logs/TEST_SIGNAL.py',
        'buy_offset_percentage': 10,
        'sell_offset_percentage': 10,
        'stop_loss_percentage': 10,
        'take_profit_percentage': 10,
        'volume': 0.10,  
    },
]
