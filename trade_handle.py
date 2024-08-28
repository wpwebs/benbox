#! .benbox/bin/python

import subprocess
import logging
import sys
import json
import time
from datetime import datetime, timedelta
import requests
import urllib3
import pandas as pd
import sqlite3
from typing import Any, Optional, Dict, List, Tuple, Union
from telegram_bot import send_message_to_topic, get_1password_secret
from tabulate import tabulate
import numpy as np

# Suppress only the single InsecureRequestWarning from urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def setup_logger() -> logging.Logger:
    """Set up a logger that outputs INFO level to the console and DEBUG level to a file."""
    logger = logging.getLogger('trade_handle')
    logger.setLevel(logging.DEBUG)

    # Ensure we don't add multiple handlers
    if not any(isinstance(handler, logging.StreamHandler) for handler in logger.handlers):
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        # console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_formatter = logging.Formatter('%(message)s')
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)  # Default to INFO level
        logger.addHandler(console_handler)
        logger.debug("Console handler added.")

    if not any(isinstance(handler, logging.FileHandler) for handler in logger.handlers):
        # File handler
        file_handler = logging.FileHandler('trade_debug.log')
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        logger.debug("File handler added.")

    return logger

def set_console_level(logger: logging.Logger, level: str) -> None:
    logger.debug(f"Executing `{set_console_level.__name__}` function with arguments: logger={logger.name}, level={level}")

    """Set the logging level of the console handler."""
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            if level.upper() == 'DEBUG':
                handler.setLevel(logging.DEBUG)
            elif level.upper() == 'INFO':
                handler.setLevel(logging.INFO)
            elif level.upper() == 'DISABLE':
                disable_console_handler(logger)
                logger.info("Console logging has been disabled.")
                return
            else:
                raise ValueError("Invalid level: choose 'DEBUG', 'INFO', or 'DISABLE'")
            logger.info(f"Console logging level set to {level.upper()}.")
            break

def disable_console_handler(logger: logging.Logger) -> None:
    logger.debug(f"Executing `{disable_console_handler.__name__}` function with arguments: logger={logger.name}")

    """Remove the console handler from the logger."""
    handlers_to_remove = [h for h in logger.handlers if isinstance(h, logging.StreamHandler)]
    for handler in handlers_to_remove:
        logger.removeHandler(handler)
        handler.close()
    logger.debug(f"Console handler removed. Remaining handlers: {logger.handlers}")
    
# Initialize the logger
logger = setup_logger()


# Set global variable
headers = {'Content-Type': 'application/json'}
DB_FILE: str = 'trade_4.db'


def load_dict_from_json_file(file_path: str) -> Dict[str, Any]:
    logger.debug(f"Executing `{load_dict_from_json_file.__name__}` function with arguments: file_path={file_path}")
    with open(file_path, 'r') as file:
        return json.load(file)

def save_to_db(cursor: sqlite3.Cursor, table: str, date: str, tickers: List[str]) -> None:
    logger.debug(f"Executing `{save_to_db.__name__}` function with arguments: table={table}, date = {date}")
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS "{table}" (
            date TEXT,
            ticker TEXT
        )
    ''')
    data = [(date, ticker) for ticker in tickers]
    cursor.executemany(f'INSERT INTO "{table}" (date, ticker) VALUES (?, ?)', data)

def column_exists(cursor: sqlite3.Cursor, table: str, column: str) -> bool:
    """Check if a column exists in a table."""
    cursor.execute(f'PRAGMA table_info("{table}")')
    columns = [info[1] for info in cursor.fetchall()]
    return column in columns

def read_from_db() -> Dict[str, Dict[str, Any]]:
    logger.debug(f"Executing `{read_from_db.__name__}` function")
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    screeners_dict: Dict[str, Dict[str, Any]] = {}

    cursor.execute("SELECT DISTINCT name FROM sqlite_master WHERE type='table'")
    filter_names = [row[0] for row in cursor.fetchall()]
    
    for filters_name in filter_names:
        if column_exists(cursor, filters_name, 'date'):
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
        else:
            logger.warning(f"Table '{filters_name}' does not have a 'date' column.")
    
    conn.close()
    return screeners_dict


def fetch_washsales(cursor: sqlite3.Cursor) -> pd.DataFrame:
    """Fetch Wash Sales from the database. Create the table if it doesn't exist."""
    try:
        cursor.execute('SELECT * FROM "Wash Sales"')
        columns = [desc[0] for desc in cursor.description]
        washsale_df = pd.DataFrame(cursor.fetchall(), columns=columns)
    except sqlite3.OperationalError as e:
        if "no such table" in str(e):
            # If the table does not exist, create it and return an empty DataFrame
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS "Wash Sales" (
                    Ticker TEXT,
                    Sold_date TEXT
                )
            ''')
            washsale_df = pd.DataFrame(columns=['Ticker', 'Sold date'])
        else:
            raise
    return washsale_df

def update_washsales(cursor: sqlite3.Cursor, washsale_df: pd.DataFrame) -> None:
    """Update Wash Sales in the database."""
    washsale_df.to_sql("Wash Sales", con=cursor.connection, if_exists="replace", index=False)


def screeners(headers: Dict[str, str], filters: Dict[str, Any]) -> List[str]:
    url = "https://seeking-alpha.p.rapidapi.com/screeners/get-results"
    querystring = {"page": "1", "per_page": "100", "type": "stock"}
    response = requests.post(url, json=filters, headers=headers, params=querystring)
    response.raise_for_status()

    return [item['attributes']['name'] for item in response.json().get('data', [])]

def group_and_combine(screeners_dict: Dict[str, Dict[str, Any]], strategies_group: Dict[str, List[str]]) -> Dict[str, Dict[str, Any]]:
    strategies: Dict[str, Dict[str, Any]] = {}

    for strategy_name, filter_names in strategies_group.items():
        combined_tickers = set()
        latest_date: Optional[str] = None
        
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



def api_request() -> List[Dict[str, Any]]:
    api_key = get_1password_secret("op://dev/rapidapi/API_KEY_2")
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "seeking-alpha.p.rapidapi.com"
    }

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')

    screeners_list: List[Dict[str, Any]] = []
    dict_list = load_dict_from_json_file('filters.json')
    
    for dictionary in dict_list:
        if isinstance(dictionary, dict):
            filters_name = dictionary.get('name')
            filters = dictionary.get('filters')
            tickers = screeners(headers, filters)

            if tickers:
                save_to_db(cursor, filters_name, today, tickers)
                screeners_list.append({
                    "Filter name": filters_name,
                    "Number of tickers": len(tickers),
                    "Tickers": tickers
                })
            else:
                logger.info(f"No tickers found for filter: {filters_name}")
        else:
            logger.error(f"Expected dictionary but got {type(dictionary)} instead.")
    
    conn.commit()
    conn.close()

    return screeners_list


def get_tickers(strategy: Optional[str] = None) -> Dict[str, Any]:
    """function to get tickers based on strategy."""
    logger.debug(f"Executing `{get_tickers.__name__}` function with arguments: strategy={strategy}")
    
    # Step 1: Read data from the database
    screeners_dict = read_from_db()
    logger.debug(f"screeners_dict: {screeners_dict}")

    # Step 2: Check if the database is empty; if yes, fetch data from the API
    if not screeners_dict:
        logger.debug("No data found in the database, fetching data from API.")
        api_request()  # Fetch and save data
        screeners_dict = read_from_db()  # Refresh the dictionary after fetching data

    # Step 3: Load the strategies group from a JSON file
    strategies_group = load_dict_from_json_file("./strategies.json")
    logger.debug(f"strategies_group: {strategies_group}")

    # Step 4: Group and combine tickers based on strategy
    strategies_dict = group_and_combine(screeners_dict, strategies_group)
    logger.debug(f"strategies_dict: {strategies_dict}")

    # Step 5: Check for outdated data and update if necessary
    today = datetime.now().strftime('%Y-%m-%d')
    data_updated = False

    for strategy_name, strategy_data in strategies_dict.items():
        if strategy_data["Latest Date"] < today:
            logger.debug(f"Data for strategy '{strategy_name}' is outdated. Fetching new data via API.")
            api_request()  # Fetch and save updated data
            strategies_dict = group_and_combine(read_from_db(), strategies_group)  # Refresh with updated data
            data_updated = True
            break

    if not data_updated:
        logger.debug("Data is already up-to-date.")

    # Step 6: Return the tickers based on the requested strategy
    if strategy:
        return strategies_dict.get(strategy, f"Strategy '{strategy}' not found.")
    else:
        return strategies_dict

def get_conid(tickers: List[str], base_url: str) -> pd.DataFrame:
    """
    Retrieve contract IDs (conid) for a list of tickers.

    Args:
        tickers (List[str]): A list of stock tickers.
        base_url (str): The base URL for the API.

    Returns:
        pd.DataFrame: A DataFrame with tickers as the index and their corresponding conids as the column.
    """
    logger.debug(f"Executing `{get_conid.__name__}` function with arguments: tickers={tickers}, base_url={base_url}")
    try:
        endpoint = '/trsrv/stocks'
        payload = {"symbols": ','.join(tickers)}
        
        response = requests.get(url=f'{base_url}{endpoint}', headers=headers, verify=False, params=payload)
        response.raise_for_status()  # Raise an error for bad responses
        time.sleep(1)
        
        data = response.json()
        data_dict = {}
        
        for key, value in data.items():
            try:
                # Check if the 'contracts' and 'isUS' keys exist
                if value and 'contracts' in value[0] and value[0]['contracts'][0].get('isUS', False):
                    data_dict[key] = value[0]['contracts'][0].get('conid', None)
                else:
                    logger.debug(f"No valid contract found for ticker: {key}")
            except (IndexError, KeyError) as e:
                logger.error(f"Error processing ticker {key}: {e}")
        
        if not data_dict:
            raise ValueError("No valid conids were found for the provided tickers.")
        
        df = pd.DataFrame.from_dict(data_dict, orient='index', columns=['conid'])
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error while fetching conids: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on failure
    except Exception as e:
        logger.error(f"An error occurred while processing conids: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on failure


def market_data(tickers: List[str], base_url: str) -> pd.DataFrame:
    """
    Retrieve the last price, market cap, and other details for a list of contract IDs (conids)
    using the Market Data snapshot API.

    Args:
        conids (List[str]): A list of contract IDs for which to retrieve market data.

    Returns:
        pd.DataFrame: A DataFrame containing the market data, including symbol, conid, last price, and market cap.
    """
    logger.debug(f"Executing `{market_data.__name__}` function with arguments: tickers={tickers}, base_url={base_url}")
    
    try:
        # Ensure that /iserver/accounts is called prior to /iserver/marketdata/snapshot
        accounts_endpoint = f'{base_url}/iserver/accounts'
        snapshot_endpoint = f'{base_url}/iserver/marketdata/snapshot'
        
        # Make the initial call to /iserver/accounts
        response = requests.get(url=accounts_endpoint, headers=headers, verify=False)
        response.raise_for_status()
        time.sleep(0.5)
        conid_list = get_conid(tickers, base_url)['conid'].tolist()
        # Prepare the payload for the market data request
        payload = {
            "conids": ','.join(map(str, conid_list)),  # Convert each conid to string and join with commas
            "fields": "55,31,7289,7282"  # Requested fields: Ticker, Last Price, Market Cap, Average Volume
        }

        # Make the first call to /snapshot to initiate market data request
        response = requests.get(url=snapshot_endpoint, headers=headers, verify=False, params=payload)
        response.raise_for_status()
        time.sleep(0.5)  # Short delay to ensure data readiness

        # Make a second call to /snapshot to ensure all fields are received
        response = requests.get(url=snapshot_endpoint, headers=headers, verify=False, params=payload)
        response.raise_for_status()

        # Parse the JSON response
        data = response.json()
        if not data:
            raise ValueError("No market data returned from the API.")

        # Filter out items that do not contain the required keys
        filtered_data = [
            item for item in data
            if all(key in item for key in ['55', 'conid', '31', '7289'])
        ]
        
        if not filtered_data:
            raise ValueError("No valid market data with the required keys were found.")

        # Convert the response to a DataFrame and select relevant columns
        df = pd.DataFrame(filtered_data)
        df = df[['55', 'conid', '31', '7289']]
        df.columns = ['Ticker', 'conid', 'Last Price', 'Market Cap']

        # Convert Ticker to string if not already and handle missing data
        df['Ticker'] = df['Ticker'].apply(str)
        
        df['Last Price'] = df['Last Price'].apply(float)
        df.fillna('N/A', inplace=True)  # Fill missing data with 'N/A'

        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error while fetching market data: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on failure
    except ValueError as e:
        logger.error(f"Value error: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on failure
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on failure

def handle_order_confirmation(response_data: dict, base_url: str) -> str:
    """
    Handle the order confirmation process if additional action is required.

    Args:
        response_data (dict): The response data from the initial order placement.
        base_url (str): The base URL for the API endpoint.

    Returns:
        str: A message indicating the result of the order confirmation process.
    """
    logger.debug(f"Executing `{handle_order_confirmation.__name__}` function with arguments: response_data={response_data}, base_url={base_url}")
    try:
        max_retries = 3  # Define maximum retries to prevent infinite loops
        retry_count = 0

        while retry_count < max_retries:
            # Check if response_data is a list and has at least one item
            if isinstance(response_data, list) and len(response_data) > 0:
                data = response_data[0]
            elif isinstance(response_data, dict):
                data = response_data
            else:
                logger.error(f"Invalid response data format: {response_data}")
                return f"Error: Invalid response data format received during order confirmation."

            # Handle error in response
            if 'error' in data:
                error_message = data.get('error', 'Unknown error occurred.')
                logger.error(f"Order confirmation failed: {error_message}. Response data: {data}")

                # Retry logic if the order confirmation fails
                retry_count += 1
                if retry_count < max_retries:
                    logger.debug(f"Retrying order confirmation. Attempt {retry_count + 1} of {max_retries}.")
                    if 'id' in data:
                        response = reply_order(data['id'], base_url)
                        response_data = response.json()
                    else:
                        logger.error(f"No 'id' found in response for retry. Aborting retries.")
                        return f"Error: Order confirmation failed. No 'id' found in response for retry."
                    time.sleep(0.5)
                    continue
                
                return f"Error: Order confirmation failed after {retry_count} attempts. {error_message}"

            # Check for order_id indicating success
            if 'order_id' in data:
                order_id = data['order_id']
                logger.debug(f"Order confirmed successfully. Order ID: {order_id}")
                return f"Order confirmed successfully. Order ID: {order_id}"

            # Check for 'id' to continue confirmation process
            if 'id' in data:
                order_reply_id = data['id']
                logger.debug(f"Additional action required. Reply ID: {order_reply_id}")

                response = reply_order(order_reply_id, base_url)
                
                # Check HTTP status code
                if response.status_code != 200:
                    logger.error(f"Failed to confirm order. HTTP Status Code: {response.status_code}, Response: {response.text}")
                    return f"Error: Failed to confirm order. HTTP Status Code: {response.status_code}"

                response_data = response.json()
                retry_count += 1

                if retry_count >= max_retries:
                    logger.error("Maximum retries reached during order confirmation.")
                    return "Error: Maximum retries reached during order confirmation."
                
                time.sleep(0.5)  # Small delay before next iteration
            else:
                logger.error(f"Unexpected response structure: {response_data}")
                return f"Error: Unexpected response structure during order confirmation."

        return "Error: Maximum retries reached without successful order confirmation."

    except Exception as e:
        logger.exception("Exception occurred during order confirmation.")
        return f"Error: Exception occurred during order confirmation. {str(e)}"

def reply_order(order_reply_id: str, base_url: str) -> requests.Response:
    """
    Reply to an order confirmation request.

    Args:
        order_reply_id (str): The ID of the order reply.
        base_url (str): The base URL for the API endpoint.

    Returns:
        requests.Response: The response from the API after replying to the order confirmation.
    """
    logger.debug(f"Executing `{reply_order.__name__}` function with arguments: order_reply_id={order_reply_id}, base_url={base_url}")
    try:
        endpoint = f'/iserver/reply/{order_reply_id}'
        payload = {
            "confirmed": True
        }
        response = requests.post(url=f'{base_url}{endpoint}', headers=headers, verify=False, json=payload)

        if response.status_code != 200:
            logger.error(f"Reply order failed. HTTP Status Code: {response.status_code}, Response: {response.text}")
            raise Exception(f"Reply order failed with status code {response.status_code}")

        logger.debug(f"Reply order successful. Response: {response.text}")
        return response

    except requests.RequestException as req_err:
        logger.exception(f"Network error occurred while replying to order confirmation: {req_err}")
        raise

    except Exception as e:
        logger.exception(f"Unexpected error occurred while replying to order confirmation: {e}")
        raise
 
def get_account_id(alias_id: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    logger.debug(f"Executing `{get_account_id.__name__}` function with arguments: alias_id={alias_id}")
    # Retrieve the port for the gateway
    try:
        master_acc = get_1password_secret(f"op://trade/{alias_id}/gateway")
        port = f'50{master_acc[-2:]}'  
        if not port:
            logger.error("Port retrieval failed. No valid port found.")
            return None, None

        # Construct the base URL for API requests
        base_url = f'https://localhost:{port}/v1/api'
        account_id = get_1password_secret(f"op://trade/{alias_id}/account")
        if not account_id:
            logger.error("Failed to retrieve account information.")
            return None, None
        
    except ValueError as ve:
        logger.error(f"Value error: {ve}")
        return None, None 
    except Exception as e:
        logger.error(f"Unexpected error while retrieving account information for account {alias_id}: {e}")
        return None, None 
    
    return account_id, base_url


def safe_convert_to_float(value: Any) -> float:
    """Safely convert a value to float. If conversion fails, return NaN."""
    logger.debug(f"Executing `{safe_convert_to_float.__name__}` function with arguments: value={value}")
    try:
        value_str = str(value).replace(',', '').strip()
        if value_str == "":
            return np.nan
        return float(value_str)
    except (ValueError, TypeError):
        return np.nan  # Return NaN if conversion fails

def format_table(df: pd.DataFrame) -> pd.DataFrame:
    """Format the DataFrame by applying specific formatting to numeric columns."""
    logger.debug(f"Executing `{format_table.__name__}` function with arguments: df={df}")

    numeric_columns = ['Position', 'mktPrice', 'mktValue', 'avgCost', 'PnL']
    
    # Format numeric columns
    for col in numeric_columns:
        df[col] = df[col].apply(lambda x: f"{safe_convert_to_float(x):,.0f}" if col == 'Position' and pd.notna(x) else
                                           f"{safe_convert_to_float(x):,.2f}" if pd.notna(x) else "")
    
    # Ensure 'conid' is consistently treated as an integer without formatting
    df['conid'] = df['conid'].apply(lambda x: str(int(safe_convert_to_float(x))) if pd.notna(x) and not pd.isna(x) and safe_convert_to_float(x) is not np.nan else "")
    
    return df

def filter_positions(data_dict: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter and rename keys in positions data and drop records with Position = 0."""
    logger.debug(f"Executing `{filter_positions.__name__}` function with arguments: data_dict={data_dict}")
    key_mapping = {
        'contractDesc': 'Ticker',
        'conid': 'conid',
        'position': 'Position',
        'mktValue': 'mktValue',
        'unrealizedPnl': 'PnL',
        'mktPrice': 'mktPrice',
        'avgCost': 'avgCost'
    }

    filtered_data = []

    logger.debug(f"Raw positions data: {json.dumps(data_dict, indent=2)}")

    for item in data_dict:
        # Skip records where 'position' is 0 or missing
        if item.get('position', 0) == 0:
            logger.debug(f"Skipping item with Position = 0: {item}")
            continue

        filtered_item = {
            new_key: item.get(original_key, 'N/A' if new_key == 'Ticker' else 0.0)
            for original_key, new_key in key_mapping.items()
        }

        filtered_data.append(filtered_item)

    if not filtered_data:
        logger.debug("No valid positions found after filtering.")
    else:
        logger.debug(f"Filtered positions: {json.dumps(filtered_data, indent=2)}")

    return filtered_data


def summary_row(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate and return a summary row for the given DataFrame with formatted numbers."""
    logger.debug(f"Executing `{summary_row.__name__}` function with arguments: df={df}")

    position_sum = df['Position'].sum()
    mkt_value_sum = df['mktValue'].sum()
    pnl_sum = df['PnL'].sum()
    avg_cost_mean = df['avgCost'].mean()
    mkt_price = mkt_value_sum / position_sum if position_sum != 0 else 0.0

    summary = pd.DataFrame([{
        'Ticker': 'TOTAL',
        'conid': '',  # No meaningful conid for a summary row, so leave it empty
        'Position': f"{position_sum:,.0f}",
        'mktValue': f"{mkt_value_sum:,.2f}",
        'PnL': f"{pnl_sum:,.2f}",
        'mktPrice': f"{mkt_price:,.2f}",
        'avgCost': f"{avg_cost_mean:,.2f}"
    }])

    return summary

def get_accounts(base_url: str) -> Optional[Dict[str, Any]]:
    """Retrieve account details from the base URL."""
    logger.debug(f"Executing `{get_accounts.__name__}` function with arguments: base_url={base_url}")
    url = f"{base_url}/portfolio/accounts"
    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"An error occurred: {e}")
        return None


def get_subaccounts(base_url: str) -> Optional[Dict[str, Any]]:
    """Retrieve subaccount details from the base URL."""
    logger.debug(f"Executing `{get_subaccounts.__name__}` function with arguments: base_url={base_url}")
    url = f"{base_url}/portfolio/subaccounts"
    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"An error occurred: {e}")
        return None


def aggregate_positions(positions_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Aggregate positions based on the Ticker, calculate the required fields, and add a summary row."""
    logger.debug(f"Executing `{aggregate_positions.__name__}` function with arguments: positions_list={positions_list}")
    if not positions_list:
        return []

    df = pd.DataFrame(positions_list)

    # Ensure all necessary columns are present
    required_columns = ['Ticker', 'Position', 'conid', 'mktValue', 'PnL', 'avgCost', 'mktPrice']
    for col in required_columns:
        if col not in df.columns:
            logger.warning(f"Column '{col}' is missing from the input data. Filling with default values.")
            df[col] = 0 if col in ['Position', 'mktValue', 'PnL', 'avgCost', 'mktPrice'] else 'N/A'

    # Aggregate the data by 'Ticker'
    aggregated_df = df.groupby('Ticker').agg({
        'Position': 'sum',
        'mktValue': 'sum',
        'PnL': 'sum',
        'avgCost': lambda x: (x * df.loc[x.index, 'Position']).sum() / df.loc[x.index, 'Position'].sum() if df.loc[x.index, 'Position'].sum() != 0 else 0,
        'mktPrice': lambda x: df.loc[x.index, 'mktValue'].sum() / df.loc[x.index, 'Position'].sum() if df.loc[x.index, 'Position'].sum() != 0 else 0.0
    }).reset_index()

    # Handle 'conid': keep the first conid encountered for each Ticker
    if 'conid' in df.columns:
        conid_map = df.drop_duplicates(subset='Ticker')[['Ticker', 'conid']].set_index('Ticker')['conid'].to_dict()
        aggregated_df['conid'] = aggregated_df['Ticker'].map(conid_map)

    # Add the summary row
    summary = summary_row(aggregated_df)
    aggregated_df = pd.concat([aggregated_df, summary], ignore_index=True)

    # Format the DataFrame before returning
    return format_table(aggregated_df).to_dict('records')


def positions(account_id: str, base_url: str) -> Optional[List[Dict[str, Any]]]:
    """Retrieve the positions for a given account."""
    logger.debug(f"Executing `{positions.__name__}` function with arguments: account_id={account_id}, base_url={base_url}")
    logger.info(f'Retrieving positions on account {account_id}')
    url = f'{base_url}/portfolio/{account_id}/positions'

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, verify=False)

            if response.status_code == 200:
                try:
                    positions_data = response.json()
                    positions_data = filter_positions(positions_data)
                    if not positions_data:
                        logger.debug(f"No positions found for account {account_id}.")
                        required_columns = ['Ticker', 'Position', 'conid', 'mktValue', 'PnL', 'avgCost', 'mktPrice']
                        positions_df = pd.DataFrame(columns=required_columns)
                        return positions_df
                    else:
                        positions_df = pd.DataFrame(positions_data)                  
                        if len(positions_df) > 1:
                            # No need to concatenate again after formatting
                            positions_df = pd.concat([positions_df, summary_row(positions_df)], ignore_index=True)
                        return format_table(positions_df)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON response for account {account_id}: {e}")
                    return None
            else:
                logger.warning(
                    f"Attempt {attempt + 1}/{max_retries} for account {account_id} failed "
                    f"with status code {response.status_code}: {response.text}"
                )
                if attempt < max_retries - 1:
                    logger.debug("Retrying...")
                else:
                    logger.error(
                        f"Failed after {max_retries} attempts for account {account_id} "
                        f"with status code {response.status_code}: {response.text}"
                    )
                    return None

        except requests.RequestException as e:
            logger.warning(
                f"Attempt {attempt + 1}/{max_retries} for account {account_id} failed with error: {e}"
            )
            if attempt < max_retries - 1:
                logger.debug("Retrying...")
            else:
                logger.error(
                    f"Failed after {max_retries} attempts for account {account_id} with error: {e}"
                )
                return None
            
def portfolio(account_id: str, base_url: str) -> pd.DataFrame:
    """Retrieve the portfolio details using the IBEAM gateway and handle cases with no positions."""
    logger.debug(f"Executing `{portfolio.__name__}` function with arguments: account_id={account_id}, base_url={base_url}")
    try:
        positions_data = positions(account_id, base_url)
        if positions_data is None:
            return "No positions found for this account."
        else:
            positions_df = pd.DataFrame(positions_data)
            if len(positions_df) > 1:
                positions_df = pd.concat([positions_df, summary_row(positions_df)], ignore_index=True)

            return format_table(positions_df)
        
    except ValueError as ve:
        logger.error(f"Value error: {ve}")
        return f"Error: {ve}"
    except Exception as e:
        logger.error(f"Unexpected error while retrieving portfolio for account ID '{account_id}': {e}")
        return f"Error: {e}"

def portfolios(account_id: str, base_url: str) -> pd.DataFrame:
    """Retrieve and aggregate the portfolio details for all subaccounts."""
    logger.debug(f"Executing `{portfolios.__name__}` function with arguments: account_id={account_id}, base_url={base_url}")
    try:
        subaccounts = get_subaccounts(base_url)
        if not subaccounts:
            return "Error: Unable to retrieve subaccounts list."

        all_positions = []

        for subaccount in subaccounts:
            account = subaccount.get('accountId')
            if not account:
                logger.warning(f"Subaccount with missing 'accountId' found: {subaccount}")
                continue

            positions_df = positions(account, base_url)
            if positions_df:
                all_positions.extend(positions_df)
            else:
                logger.warning(f"No positions found for subaccount {account}")

        if not all_positions:
            return "No positions found across all subaccounts."

        aggregated_positions = aggregate_positions(all_positions)
        if not aggregated_positions:
            return "No valid positions after aggregation."
        else:
            aggregated_df = pd.DataFrame(aggregated_positions)
            if len(aggregated_df) > 1:
                aggregated_df = pd.concat([aggregated_df, summary_row(aggregated_df)], ignore_index=True)

            return format_table(aggregated_df)

    except ValueError as ve:
        logger.error(f"Value error: {ve}")
        return f"Error: {ve}"
    except Exception as e:
        logger.error(f"Unexpected error while retrieving portfolio for account ID '{account_id}': {e}")
        return f"Error: {e}"

    
def account_summary(account_id: str, base_url: str) -> pd.DataFrame:
    """
    Retrieve the account summary, including total cash value.

    Args:
        account_id (str): The account ID to retrieve the summary for.

    Returns:
        Optional[Dict[str, Any]]: A dictionary containing the filtered summary data or None if retrieval fails.
    """
    logger.debug(f"Executing `{account_summary.__name__}` function with arguments: account_id={account_id}, base_url={base_url}")
    try:

        # Build the URL for the summary endpoint
        url = f"{base_url}/portfolio/{account_id}/summary"
        logger.debug(f"Requesting summary from URL: {url}")
        
        # Make the request to the API
        response = requests.get(url, headers={'Content-Type': 'application/json'}, verify=False)
        response.raise_for_status()
        
        # Parse the JSON response
        summary_data = response.json()
        # logger.info(f"Received summary data: {summary_data}")

        # Check if summary data is available
        if not summary_data:
            logger.warning(f"No portfolio summary found for account {account_id}.")
            return None
        
        # Convert to DataFrame and apply filtering logic
        summary_df = pd.DataFrame.from_dict(summary_data, orient='index')
        if 'amount' not in summary_df.columns:
            logger.warning(f"Expected 'amount' key missing in the summary data for account {account_id}.")
            return None
        
        summary_df = summary_df[~summary_df.index.str.contains('-s')]
        summary_df = summary_df[['amount']]
        summary_df['amount'] = pd.to_numeric(summary_df['amount'], errors='coerce').fillna(0)
        summary_df = summary_df[summary_df['amount'] != 0]
        summary_df['amount'] = summary_df['amount'].apply(lambda x: f"{x:,.2f}")

        # Convert back to dictionary format
        # summary_dict = summary_df['amount'].to_dict()

        # Return the processed summary data
        return summary_df

    except ValueError as ve:
        logger.error(f"Value error: {ve}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error while retrieving portfolio summary for account {account_id}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error while retrieving portfolio summary for account {account_id}: {e}")
        return None


def clean_numeric(value: Union[str, float, int]) -> float:
    import re
    """
    Clean a string by removing non-numeric characters and convert it to a float.
    If the value is already numeric, return it as a float.
    
    Args:
        value (Union[str, float, int]): The value to clean.
        
    Returns:
        float: The cleaned numeric value. Returns NaN if conversion fails.
    """
    logger.debug(f"Executing `{clean_numeric.__name__}` function with arguments: value={value}")
    if isinstance(value, (float, int)):
        return float(value)
    
    # Remove non-numeric characters except for the decimal point
    cleaned_value: str = re.sub(r'[^\d.]', '', value)
    
    try:
        return float(cleaned_value)
    except ValueError:
        return float('nan')


def rebalance(
    summary_df: pd.DataFrame, 
    positions_df: pd.DataFrame, 
    market_data_df: pd.DataFrame, 
    tolerance: float = 0.10
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    
    logger.debug(f"Executing `{rebalance.__name__}` function with arguments: summary_df={summary_df}, \
                positions_df={positions_df}, \
                market_data_df={market_data_df}, \
                tolerance={tolerance} ")
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Fetch existing wash sales data
    washsale_df = fetch_washsales(cursor)
    
    if washsale_df.empty:
        logger.debug("No wash sale data found. Proceeding without wash sale adjustments.")
        washsale_df = pd.DataFrame(columns=['Ticker', 'Sold date'])

    required_summary_cols = ['totalcashvalue', 'netliquidation']
    if not all(col in summary_df.index for col in required_summary_cols):
        logger.error("Error: Required columns missing in summary_df.")
        return None, None

    required_positions_cols = ['Ticker', 'conid', 'Position', 'mktValue', 'PnL']
    if not all(col in positions_df.columns for col in required_positions_cols):
        logger.error("Error: Required columns missing in positions_df.")
        return None, None

    required_market_data_cols = ['Ticker', 'conid', 'Last Price']
    if not all(col in market_data_df.columns for col in required_market_data_cols):
        logger.error("Error: Required columns missing in market_data_df.")
        return None, None

    totalcashvalue = clean_numeric(summary_df.loc['totalcashvalue']['amount'])
    netliquidation = clean_numeric(summary_df.loc['netliquidation']['amount'])

    if pd.isna(totalcashvalue) or pd.isna(netliquidation):
        logger.error("Error: Unable to convert 'totalcashvalue' or 'netliquidation' to a numeric type.")
        return None, None

    market_data_df['Last Price'] = market_data_df['Last Price'].apply(clean_numeric)
    positions_df['Position'] = pd.to_numeric(positions_df['Position'], errors='coerce')
    positions_df['mktValue'] = pd.to_numeric(positions_df['mktValue'], errors='coerce')
    positions_df['PnL'] = pd.to_numeric(positions_df['PnL'], errors='coerce')

    if market_data_df['Last Price'].isna().any():
        logger.error("Error: Unable to convert some 'Last Price' values to a numeric type.")
        return None, None

    # Step 1: Calculate target market value for equal weighting
    num_tickers = len(market_data_df)
    target_market_value = (netliquidation * 0.8) / num_tickers

    market_data_df['Target Market Value'] = target_market_value
    market_data_df['Target Shares'] = round(market_data_df['Target Market Value'] / market_data_df['Last Price'])

    # Step 2: Merge positions_df with market_data_df using an outer join to include all tickers from market_data_df
    rebalance_df = pd.merge(
        market_data_df[['Ticker', 'conid', 'Last Price', 'Target Shares', 'Target Market Value']],
        positions_df[['Ticker', 'conid', 'Position', 'mktValue', 'PnL']],
        on=['Ticker', 'conid'],
        how='outer',
        suffixes=('', '_pos')
    )

    # Step 3: Handle NaN values
    rebalance_df.fillna({
        'Position': 0,
        'mktValue': 0,
        'PnL': 0,
        'Target Shares': 0,
        'Target Market Value': 0
    }, inplace=True)

    # Step 4: Calculate Trade Quantity
    rebalance_df['Trade Quantity'] = rebalance_df['Target Shares'] - rebalance_df['Position']

    # Step 5: For tickers that are only in positions_df, Trade Quantity should be -Position
    rebalance_df.loc[rebalance_df['Target Shares'] == 0, 'Trade Quantity'] = -rebalance_df['Position']

    # Step 6: Calculate the difference between target and current market values
    rebalance_df['Difference'] = rebalance_df['Target Market Value'] - rebalance_df['mktValue']
    rebalance_df['Difference (%)'] = rebalance_df['Difference'] / rebalance_df['Target Market Value']

    logger.debug(f"rebalance_df with Trade Quantity:\n{rebalance_df}")

    # Step 7: Filter tickers that need rebalancing based on tolerance
    trade_df = rebalance_df[
        (rebalance_df['Difference (%)'].abs() > tolerance) |
        (rebalance_df['Target Shares'] == 0)  # For tickers only in positions_df
    ].copy()

    # Ensure all `conid` values from positions_df are preserved in trade_df
    trade_df['conid'] = trade_df.apply(
        lambda row: positions_df.loc[positions_df['Ticker'] == row['Ticker'], 'conid'].values[0]
        if pd.isna(row['conid']) and not positions_df[positions_df['Ticker'] == row['Ticker']].empty
        else row['conid'],
        axis=1
    )

    logger.debug(f"trade_df after filtering and ensuring conid:\n{trade_df}")

    # Convert 'Sold date' to datetime in washsale_df
    washsale_df['Sold date'] = pd.to_datetime(washsale_df['Sold date'], errors='coerce')

    current_date = datetime.now()
    new_washsales_df = trade_df[(trade_df['PnL'] < 0) & (trade_df['Trade Quantity'] < 0)][['Ticker', 'conid']]
    new_washsales_df['Sold date'] = current_date.strftime('%Y-%m-%d')

    washsale_df = washsale_df[washsale_df['Sold date'] + timedelta(days=31) >= current_date]

    for ticker in new_washsales_df['Ticker']:
        if ticker in washsale_df['Ticker'].values:
            washsale_df.loc[washsale_df['Ticker'] == ticker, 'Sold date'] = current_date.strftime('%Y-%m-%d')
        else:
            washsale_df = pd.concat([washsale_df, new_washsales_df])

    trade_df = trade_df[~trade_df['Ticker'].isin(washsale_df['Ticker'])]

    # Update the database with the new Wash Sales data
    update_washsales(cursor, washsale_df)

    conn.commit()
    conn.close()

    return trade_df, washsale_df

def place_market_order(account_id: str, conid: int, quantity: int, base_url: str) -> str:
    """
    Place a market order and handle the confirmation process automatically.

    Args:
        account_id (str): The account ID for placing the order.
        conid (int): The contract ID of the instrument to trade.
        quantity (int): The number of shares to buy (positive) or sell (negative).
        base_url (str): The base URL for the API endpoint.

    Returns:
        str: A message indicating the result of the order placement.
    """
    
    logger.debug(f"Executing `{place_market_order.__name__}` function with arguments: \
                account_id={account_id} \
                conid={conid} \
                quantity={quantity} \
                base_url={base_url} ")
    
    try:
        endpoint = f'/iserver/account/{account_id}/orders'
        payload = {
            "orders": [
                {
                    "acctId": account_id,
                    "conid": int(conid),
                    "quantity": int(abs(quantity)),
                    "side": "BUY" if quantity > 0 else "SELL",
                    "orderType": 'MKT',
                    "tif": 'DAY',
                }
            ]
        }

        # Step 1: Place Order
        response = requests.post(url=f'{base_url}{endpoint}', headers=headers, verify=False, json=payload)
        response_data = response.json()
        time.sleep(0.5)

        if response.status_code == 200:
            if 'order_id' in response.text:
                order_id = response_data[0]['order_id']
                logger.debug(f"Order placed successfully. Order ID: {order_id}")
                return f"Order placed successfully. Order ID: {order_id}"
            elif 'id' in response.text:
                return handle_order_confirmation(response_data, base_url)
            else:
                logger.error(f"Unexpected response format: {response.text}")
                return f"Error placing order: Unexpected response format."
        else:
            logger.error(f"Error placing order: {response.status_code} - {response.text}")
            return f"Error placing order: {response.status_code} - {response.text}"

    except Exception as e:
        logger.error(f"Failed to execute trade: {e}")
        return f"Error: Failed to execute trade. {e}"

def execute_trades(trade_df: pd.DataFrame, account_id: str, base_url: str) -> None:
    """
    Loop through the trade_df and place a market order for each trade.

    Args:
    - trade_df (pd.DataFrame): DataFrame containing the trades to be executed.
    - account_id (str): The account ID where the orders will be placed.
    - base_url (str): The base URL for the API endpoint.

    Returns:
    - None
    """
    
    logger.debug(f"Executing `{execute_trades.__name__}` function with arguments: \
            trade_df={trade_df} \
            base_url={base_url} \
            account_id={account_id}")
    
    for index, row in trade_df.iterrows():
        conid = int(row['conid'])
        quantity = int(row['Trade Quantity'])

        # Log the order details
        logger.info(f"Placing market order for conid {conid} with quantity {quantity}.")

        try:
            # Call the place_market_order function
            order_response = place_market_order(account_id, conid, quantity, base_url)

            # Log the response
            logger.info(f"Order response: {order_response}")

        except Exception as e:
            logger.error(f"Failed to place order for conid {conid}: {str(e)}")

def cancel_order(account_id: str, base_url: str, order_id: Union[str, int]) -> dict:
    """
    Cancels an order for a given account.

    Args:
        account_id (str): The ID of the account for which the order needs to be canceled.
        base_url (str): The base URL for the API.
        order_id (Union[str, int]): The ID of the order to be canceled.
        headers (Optional[Dict[str, str]]): Optional headers to include in the request. Defaults to None.
        verify_ssl (bool): Whether to verify SSL certificates. Default is False.

    Returns:
        dict: A dictionary containing the response from the API. If an error occurs, the dictionary will contain error details.
    """
    logger.debug(f"Executing `{cancel_order.__name__}` function with arguments: \
            account_id={account_id} \
            base_url={base_url} \
            order_id={order_id}")
    try:
        # Define the endpoint for cancelling the order
        endpoint = f'/iserver/account/{account_id}/order/{str(order_id)}'

        # Send DELETE request to cancel the order
        response = requests.delete(url=f'{base_url}{endpoint}', headers=headers, verify=False)

        # Check if the request was successful
        if response.status_code != 200:
            response.raise_for_status()  # Raises an HTTPError if the status code is not 200

        # Return the response as a JSON dictionary
        return response.json()

    except requests.exceptions.RequestException as e:
        logger.error(f"An HTTP error occurred: {e}")
        return {"error": str(e)}
    except ValueError as ve:
        logger.error(f"Value error: {ve}")
        return {"error": str(ve)}
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return {"error": str(e)}


def cancel_all_orders(account_id: str, base_url: str) -> None:
    """
    Cancels all live orders with a specific status for a given account.

    Args:
        account (dict): Dictionary containing account information, including 'accountId'.

    Returns:
        None
    """
    logger.debug(f"Executing `{cancel_all_orders.__name__}` function with arguments: \
            account_id={account_id} \
            base_url={base_url}")
    
    try:
        # Fetch all live orders
        live_orders_df = live_orders(base_url)
        
        # Loop through the filtered orders and cancel each one
        for _, row in live_orders_df.iterrows():
            order_id = row['orderId']
            try:
                response = cancel_order(account_id, base_url, order_id)
                logger.debug(f"Order {order_id} cancel response: {response}")
            except Exception as e:
                logger.error(f"Failed to cancel order {order_id}: {e}")
    except Exception as e:
        logger.error(f"An error occurred during order cancellation: {e}")


def live_orders(base_url: str, filter_list: list = ['Inactive', 'PreSubmitted', 'Submitted', 'PendingCancel']) -> pd.DataFrame:
    """
    Fetches live orders for a given account.

    Args:
        base_url (str): The base URL for the API.
        filter_list (list): List of statuses to filter out from the DataFrame. Default is an empty list.

    Returns:
        pd.DataFrame: A DataFrame containing the live orders for the account.
    """
    logger.debug(f"Executing `{live_orders.__name__}` function with arguments: \
            base_url={base_url} \
            filter_list={filter_list}")
    
    try:
        orders_endpoint = f'/iserver/account/orders'

        # Send GET request to fetch live orders
        response = requests.get(url=f'{base_url}{orders_endpoint}', headers=headers, verify=False)

        # Check if the request was successful
        if response.status_code != 200:
            raise Exception(f"Failed to retrieve live orders: {response.status_code} {response.text}")

        # Parse the JSON response
        response_json = response.json()

        # Ensure 'orders' key exists in the response
        if 'orders' not in response_json or not response_json['orders']:
            logger.debug("No orders found in the response.")
            return pd.DataFrame()  # Return an empty DataFrame if no orders are present

        # Convert the list of orders into a DataFrame
        orders_df = pd.DataFrame(response_json['orders'])

        # Keep only the desired columns
        desired_columns = ['account', 'orderId', 'ticker', 'remainingQuantity', 'totalSize', 'status', 'origOrderType', 'side', 'avgPrice']
        orders_df = orders_df[desired_columns]

        # Optional: Filter out specific order statuses if needed
        if filter_list:
            orders_df = orders_df[orders_df['status'].isin(filter_list)]

        return orders_df

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error


def cash_all(account_id: str, base_url: str) -> Optional[pd.DataFrame]:
    
    logger.debug(f"Executing `{live_orders.__name__}` function with arguments: \
            base_url={base_url} \
            account_id={account_id}")
    try:
        # Retrieve positions
        positions_data = positions(account_id, base_url)

        # Ensure positions_data is a DataFrame
        if isinstance(positions_data, list):
            positions_df = pd.DataFrame(positions_data)
        else:
            positions_df = positions_data  # Assuming it's already a DataFrame

        if positions_df is None or positions_df.empty:
            logging.debug(f"No equity positions found for account ID {account_id}.")
            return None

        # Convert columns to numeric, handling errors
        positions_df['Position'] = pd.to_numeric(positions_df['Position'], errors='coerce').fillna(0)
        positions_df['PnL'] = pd.to_numeric(positions_df['PnL'], errors='coerce').fillna(0)

        # Filter for positions with negative PnL (wash sales)
        washsale_df: pd.DataFrame = positions_df[positions_df['PnL'] < 0][['Ticker', 'PnL']].copy()
        washsale_df['sold_date'] = datetime.now().strftime('%Y-%m-%d')

        if washsale_df.empty:
            logging.debug(f"No wash sales found for account ID {account_id}.")

        # Place market orders to sell all positions
        for _, row in positions_df.iterrows():
            conid = row['conid']
            quantity = -row['Position']
            
            if quantity != 0:
                place_market_order(account_id, conid, quantity, base_url)
                logging.debug(f"Placed market order for {quantity} units of {conid}.")

        return washsale_df if not washsale_df.empty else None

    except KeyError as e:
        logging.error(f"Key error: {e}. Ensure the required columns exist in the positions data.")
        return None
    except Exception as e:
        logging.error(f"Unexpected error while processing cash_all for account ID {account_id}: {e}")
        return None

def format_tickers_message(result: Union[Dict[str, Any], List[str]], strategy: Optional[str]) -> str:
    """Format the tickers result into a message for display."""
    
    logger.debug(f"Executing `{format_tickers_message.__name__}` function with arguments: \
            result={result} \
            strategy={strategy}")
    
    if strategy:
        latest_date = result.get('Latest Date', 'Unknown Date')
        number_of_tickers = result.get('Number of tickers', 0)
        tickers_list = result.get('Tickers', [])

        message = f"\n\n**Tickers Results were scanned at {latest_date}:**\n"
        message += f"\nStrategy: {strategy}\n"
        message += f"Number of tickers: {number_of_tickers}\n"
        message += f"Tickers: {', '.join(tickers_list)}\n"
    else:
        message = f"\n\n**Tickers Results for all strategies:**\n"
        for strategy_name, data in result.items():
            latest_date = data.get('Latest Date', 'Unknown Date')
            number_of_tickers = data.get('Number of tickers', 0)
            tickers_list = data.get('Tickers', [])

            message += f"\nStrategy: {strategy_name}\n"
            message += f"Latest Date: {latest_date}\n"
            message += f"Number of tickers: {number_of_tickers}\n"
            message += f"Tickers: {', '.join(tickers_list)}\n"

    return message


def pretty_df(df: pd.DataFrame, index: bool = False) -> str:
    
    logger.debug(f"Executing `{pretty_df.__name__}` function with arguments: df={df}, index={index}")
    
    # Handle an empty DataFrame
    if df.empty:
        return df
    
    # Convert index to column(s) if index=True
    if index:
        df = df.reset_index()

    # Determine the number of columns
    num_columns = df.shape[1]

    # Create colalign tuple: first column left-aligned, rest right-aligned
    colalign = ("left",) + ("right",) * (num_columns - 1)

    # Calculate column widths based on the maximum length of data in each column
    col_widths = []
    for col in df.columns:
        max_width = max(df[col].astype(str).apply(len).max(), len(col))
        col_widths.append(max_width)

    # Format the DataFrame using tabulate
    formatted_table = tabulate(
        df,
        headers='keys',
        tablefmt='pretty',
        showindex=False,  # Do not show the index
        colalign=colalign,
        stralign='right',  # General alignment
        maxcolwidths=col_widths
    )

    return f"\n{formatted_table}"

def main(args: List[str]) -> None:
    """Main function to handle command line arguments and execute the appropriate function."""
    if len(args) < 3:  # args structure: args += [chat_id, topic_id, bot_token]
        logger.error("Insufficient arguments provided. Expected at least 3 arguments.")
        sys.exit(1)

    message = None  # Initialize the message variable

    try:
        func_name = args[0].lower()

        # Map function names to actual functions
        func_mapping = {
            'logger': set_console_level,
            'tickers': get_tickers,
            'positions': positions,
            'portfolios': portfolios,
            'account': account_summary,
            'rebalance': rebalance,
            'orders': live_orders,
            'cash_all': cash_all,
            'cancel_all_orders': cancel_all_orders,
            # Add other function mappings as needed
        }

        if func_name not in func_mapping:
            raise AttributeError(f"Function '{func_name}' not found in func_mapping.")

        func = func_mapping[func_name]

        topic_id = args[-2]
        chat_id = args[-3]
        bot_token = args[-1]

        # Validate that topic_id, chat_id, and bot_token are present
        if not topic_id or not chat_id or not bot_token:
            raise ValueError("Missing required arguments: chat_id, topic_id, or bot_token.")

        # Retrieve the trading group secret
        trading_group_secret = get_1password_secret("op://dev/Telegrambot/trading")
        trading_group = json.loads(trading_group_secret) if trading_group_secret else None
        if not trading_group:
            raise ValueError("Unable to retrieve trading group secret.")

        # Create a reverse mapping from thread_id to topic_name
        THREAD_ID_TO_TOPIC_NAME: Dict[str, str] = {v: k for k, v in trading_group.items()}

        # Map topic_id to alias_id
        alias_id = THREAD_ID_TO_TOPIC_NAME.get(str(topic_id), None)
        if not alias_id:
            raise ValueError(f"No alias found for topic_id: {topic_id}")

        # Retrieve account_id from alias_id
        account_id, base_url = get_account_id(alias_id)

        # Execute the specified function
        logger.info(f"Executing `{func.__name__}` function with account_id='{account_id}' and base_url='{base_url}'.")

        if func_name == 'logger' and len(args) > 1:
            logger_command = args[1].lower()
            logger.info(f"Setting logger level to '{logger_command}'.")
            func(logger, logger_command)
        elif func_name == 'tickers':
            strategy = get_1password_secret(f"op://trade/{alias_id}/strategy")
            if not strategy:
                logger.info(f"Strategy not found for alias_id: {alias_id}")
            tickers = func(strategy)
            message = format_tickers_message(tickers, strategy)
        elif func_name == 'rebalance':
            
            strategy = get_1password_secret(f"op://trade/{alias_id}/strategy")
            if not strategy:
                raise ValueError(f"Strategy not found for alias_id: {alias_id}")
            
            logger.info(f'\nGetting tickers...')
            tickers = get_tickers(strategy)
            logger.info(format_tickers_message(tickers, strategy))
            
            logger.info('\nGetting Market data ...')
            
            market_data_df = market_data(tickers.get('Tickers'), "https://localhost:5061/v1/api")
            logger.info('Market data:')
            logger.info(pretty_df(market_data_df[['Ticker','Last Price']]))
            
            logger.info('\nGetting Account summary ...')
            summary_df = account_summary(account_id, base_url)
            logger.info('Account summary:')
            summary_df_formated = summary_df.reset_index()
            logger.info(pretty_df(summary_df_formated))
            
            logger.info('\nGetting Account positions ...')
            positions_df = positions(account_id, base_url)
            logger.info('Account positions:')
            # logger.info(pretty_df(positions_df[['Ticker','Position','PnL']]))
            logger.info(pretty_df(positions_df))
            
            logger.info('\nRebalacing ...')
            trade_df, washsale_df = rebalance(summary_df, positions_df, market_data_df, tolerance = 0.10)
            logger.info('\nTrade tickers ...')
            # trade_df = trade_df[['Ticker', 'Trade Quantity']]
            logger.info(pretty_df(trade_df[['Ticker','Target Shares','Trade Quantity']]))
            
            logger.info('\nWashsale tickers ...')
            washsale_df['Expiration date'] = (datetime.now() + timedelta(days=31)).strftime('%Y-%m-%d')   
            logger.info(pretty_df(washsale_df))
            
            execute_trades(trade_df, account_id, base_url)
 
        else:
            result = func(account_id, base_url)
            if isinstance(result, pd.DataFrame):
                if func_name == 'account':
                    message = pretty_df(result,index=True)
                else:
                    message = pretty_df(result)
            else:
                message = str(result)
        if message:
            logger.info(message)

    except AttributeError as ae:
        logger.error(f"AttributeError: {ae}. Check if the function name is correctly mapped.")
        print(f"Error: {ae}")
        sys.exit(1)
    except ValueError as ve:
        logger.error(f"ValueError: {ve}.")
        print(f"Error: {ve}")
        sys.exit(1)
    except Exception as ex:
        logger.error(f"Unexpected error: {ex}.")
        print(f"Error: {ex}")
        sys.exit(1)

if __name__ == "__main__":
    main(sys.argv[1:])