#! .benbox/bin/python

import requests
from datetime import datetime
import sqlite3
import logging
import sys
import json

from telegram_trade import send_message_to_topic, get_1password_secret

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Global variable for database file
db_file = 'screeners_4.db'

def load_dict_from_json_file(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def save_to_db(cursor, table, date, tickers):
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS "{table}" (
            date TEXT,
            ticker TEXT
        )
    ''')
    data = [(date, ticker) for ticker in tickers]
    cursor.executemany(f'INSERT INTO "{table}" (date, ticker) VALUES (?, ?)', data)

def get_tickers(headers, filters):
    URL = "https://seeking-alpha.p.rapidapi.com/screeners/get-results"
    QUERYSTRING = {"page": "1", "per_page": "100", "type": "stock"}
    response = requests.post(URL, json=filters, headers=headers, params=QUERYSTRING)
    response.raise_for_status()

    return [item['attributes']['name'] for item in response.json().get('data', [])]

def read_from_db():
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    screeners_dict = {}

    cursor.execute("SELECT DISTINCT name FROM sqlite_master WHERE type='table'")
    filter_names = [row[0] for row in cursor.fetchall()]
    
    for filters_name in filter_names:
        cursor.execute(f'''
            SELECT date, ticker
            FROM "{filters_name}"
            WHERE date = (SELECT MAX(date) FROM "{filters_name}")
        ''')
        rows = cursor.fetchall()
        
        if rows:
            latest_date = rows[0][0]
            tickers = [row[1] for row in rows]
            
            screeners_dict[filters_name] = {
                "Latest Date": latest_date,
                "Number of tickers": len(tickers),
                "Tickers": tickers
            }
        else:
            logger.warning(f"No tickers found for filter: {filters_name}")
    
    conn.close()
    return screeners_dict

def group_and_combine(screeners_dict, strategies_group):
    strategies = {}

    for strategy_name, filter_names in strategies_group.items():
        combined_tickers = set()
        latest_date = None
        
        for filter_name in filter_names:
            if filter_name in screeners_dict:
                combined_tickers.update(screeners_dict[filter_name]["Tickers"])
                
                filter_date = screeners_dict[filter_name]["Latest Date"]
                if latest_date is None or filter_date > latest_date:
                    latest_date = filter_date
        
        if combined_tickers:
            strategies[strategy_name] = {
                "Latest Date": latest_date,
                "Number of tickers": len(combined_tickers),
                "Tickers": sorted(list(combined_tickers))
            }
    
    return strategies

def api_request():
    API_KEY = get_1password_secret("op://dev/rapidapi/API_KEY_2")
    headers = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": "seeking-alpha.p.rapidapi.com"
    }

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')

    screeners_list = []
    dict_list = load_dict_from_json_file('filters.json')
    
    for dictionary in dict_list:
        filters_name = dictionary.get('name')
        filters = dictionary.get('filters')
        tickers = get_tickers(headers, filters)

        if tickers:
            save_to_db(cursor, filters_name, today, tickers)
            screeners_list.append({
                "Filter name": filters_name,
                "Number of tickers": len(tickers),
                "Tickers": tickers
            })
        else:
            logger.info(f"No tickers found for filter: {filters_name}")
    
    conn.commit()
    conn.close()

    return screeners_list

def tickers(strategy: str = None):
    screeners_dict = read_from_db()
    strategies_group = load_dict_from_json_file("./strategies.json")
    strategies_dict = group_and_combine(screeners_dict, strategies_group)

    today = datetime.now().strftime('%Y-%m-%d')
    data_updated = False

    for strategy_name, strategy_data in strategies_dict.items():
        if strategy_data["Latest Date"] < today:
            logger.info(f"Data for strategy '{strategy_name}' is outdated. Fetching new data via API.")
            new_data = api_request()
            strategies_dict = group_and_combine(read_from_db(), strategies_group)  # Refresh with updated data
            data_updated = True
            break

    if not data_updated:
        logger.info("Data is already up-to-date.")

    if strategy:
        return strategies_dict.get(strategy, f"Strategy '{strategy}' not found.")
    else:
        return strategies_dict

def main(args):

    try:
        func_name = 'tickers'
        func_args = args[1:]
        
        # Mapping the topic_id with strategy
        trading_group_secret = get_1password_secret("op://dev/Telegrambot/trading")
        trading_group = json.loads(trading_group_secret) if trading_group_secret else None
        
        # Create a reverse mapping from thread_id to topic_name
        THREAD_ID_TO_TOPIC_NAME = {v: k for k, v in trading_group.items()}
        logger.info(f"THREAD_ID_TO_TOPIC_NAME: {THREAD_ID_TO_TOPIC_NAME}")
        try:
            acc_id = THREAD_ID_TO_TOPIC_NAME.get(str(args[2])) # args[2] is topic_id
            strategy = get_1password_secret(f"op://trade/{acc_id}/strategy")
            logger.info(strategy)
        except Exception:
            strategy = None
            
        if func_name not in globals():
            raise AttributeError(f"Function '{func_name}' not found.")
        
        func = globals()[func_name]
        result = func(strategy)
        
        if isinstance(result, (dict, list)):
            if strategy:
                # If strategy is specified, work with the specific strategy data
                latest_date = result.get('Latest Date', 'Unknown Date')
                number_of_tickers = result.get('Number of tickers', 0)
                tickers_list = result.get('Tickers', [])
                
                # Format the tickers data into a message
                message = f"**Tickers Results were scanned at {latest_date}:**\n"
                message += f"\nStrategy: {strategy}\n"
                message += f"Number of tickers: {number_of_tickers}\n"
                message += f"Tickers: {', '.join(tickers_list)}\n"
            else:
                # If no strategy is specified, show all strategies
                message = f"**Tickers Results for all strategies:**\n"
                for strategy_name, data in result.items():
                    latest_date = data.get('Latest Date', 'Unknown Date')
                    number_of_tickers = data.get('Number of tickers', 0)
                    tickers_list = data.get('Tickers', [])
                
                    # Format the tickers data for each strategy into the message
                    message += f"\nStrategy: {strategy_name}\n"
                    message += f"Latest Date: {latest_date}\n"
                    message += f"Number of tickers: {number_of_tickers}\n"
                    message += f"Tickers: {', '.join(tickers_list)}\n"
        else:
            message = str(result)

        print(message)
    
    except Exception as ex:
        logger.error(f"Unexpected error: {ex}")
        print(f"Error: {ex}")

if __name__ == "__main__":
    main(sys.argv[1:])
