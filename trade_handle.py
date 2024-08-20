#!/benbox/bin/python

import subprocess
import logging
import sys
import json
import requests
import urllib3
import pandas as pd

# Suppress only the single InsecureRequestWarning from urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from telegram_trade import get_1password_secret

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def gateway(account_id):
    """Retrieve credentials for a given account."""
    try:
        master_acc = get_1password_secret(f"op://trade/{account_id}/gateway")
        port = f'50{master_acc[-2:]}'
        return port
    except Exception as e:
        logger.error(f"Failed to retrieve credentials for account ID '{account_id}': {e}")
        return None


def safe_convert_to_float(value):
    """Safely convert a value to float. If conversion fails, return 0.0."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def filter_positions(data_dict):
    """Filter and rename keys in positions data."""
    key_mapping = {
        'contractDesc': 'Ticker',
        'position': 'Position',
        'mktValue': 'mktValue',
        'unrealizedPnl': 'PnL',
        'mktPrice': 'mktPrice',
        'avgCost': 'avgCost'
    }

    filtered_data = []
    valid_positions_found = False  # Track if any valid positions are found

    logger.debug(f"Raw positions data: {json.dumps(data_dict, indent=2)}")

    for item in data_dict:
        filtered_item = {}

        # Iterate through the key_mapping to filter and rename keys
        for original_key, new_key in key_mapping.items():
            value = item.get(original_key)
            if value is None:
                # Handle missing data by setting it to 'N/A' or 0.0
                value = 'N/A' if new_key == 'Ticker' else 0.0
            filtered_item[new_key] = value

        # Check if this item has a valid Ticker and Position > 0
        if filtered_item['Ticker'] != 'N/A' and filtered_item['Position'] >= 0:
            valid_positions_found = True
            filtered_data.append(filtered_item)

    if not valid_positions_found:
        logger.info("No valid positions found after filtering.")

    return filtered_data

def format_table(df):
    # Ensure numeric columns are properly converted before formatting
    df['Position'] = df['Position'].apply(safe_convert_to_float)
    df['mktPrice'] = df['mktPrice'].apply(safe_convert_to_float)
    df['mktValue'] = df['mktValue'].apply(safe_convert_to_float)
    df['avgCost'] = df['avgCost'].apply(safe_convert_to_float)
    df['PnL'] = df['PnL'].apply(safe_convert_to_float)

    # Select relevant columns and format the DataFrame
    df = df[['Ticker', 'Position', 'mktValue', 'PnL', 'mktPrice', 'avgCost']]

    # Apply custom formatting to numeric columns
    df['Position'] = df['Position'].apply(lambda x: f"{x:,.0f}")
    df['mktValue'] = df['mktValue'].apply(lambda x: f"{x:,.2f}")
    df['PnL'] = df['PnL'].apply(lambda x: f"{x:,.2f}")
    df['mktPrice'] = df['mktPrice'].apply(lambda x: f"{x:,.2f}")
    df['avgCost'] = df['avgCost'].apply(lambda x: f"{x:,.2f}")

    # Convert the DataFrame to a string with tabular formatting
    formatted_table = df.to_string(index=False)

    return f"```\n{formatted_table}\n```"

def get_positions(account, base_url):
    """Retrieve the positions for a given account."""
    url = f'{base_url}/portfolio/{account}/positions'
    headers = {'Content-Type': 'application/json'}

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, verify=False)

            if response.status_code == 200:
                try:
                    positions_data = response.json()
                    positions_data = filter_positions(positions_data)
                    if not positions_data:
                        logger.info(f"No positions found for account {account}.")
                        return None
                    return positions_data
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON response for account {account}: {e}")
                    return None
            else:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} for account {account} failed with status code {response.status_code}: {response.text}")
                if attempt < max_retries - 1:
                    logger.info("Retrying...")
                else:
                    logger.error(f"Failed after {max_retries} attempts for account {account} with status code {response.status_code}: {response.text}")
                    return None

        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries} for account {account} failed with error: {e}")
            if attempt < max_retries - 1:
                logger.info("Retrying...")
            else:
                logger.error(f"Failed after {max_retries} attempts for account {account} with error: {e}")
                return None

def portfolio(account_id):
    """Retrieve the portfolio details using the IBEAM gateway and handle cases with no positions."""
    try:
        port = gateway(account_id)
        base_url = f'https://localhost:{port}/v1/api'
        if not port:
            raise ValueError("Port retrieval failed. No valid port found.")

        account = get_1password_secret(f"op://trade/{account_id}/account")
        if not account:
            return "Error: Unable to retrieve account."

        # Retrieve positions for the account
        positions_data = get_positions(account, base_url)
        if positions_data is None:
            return "No positions found for this account."
        else:
            positions_df = pd.DataFrame(positions_data)
            if len(positions_df) > 1:
                # Calculate summary values for the entire dataset
                # Concatenate the summary row to the aggregated DataFrame
                positions_df = pd.concat([positions_df, summary_row(positions_df)], ignore_index=True)
        
            # Format and return the positions data
            return format_table(positions_df)

    except ValueError as ve:
        logger.error(f"Value error: {ve}")
        return f"Error: {ve}"
    except Exception as e:
        logger.error(f"Unexpected error while retrieving portfolio for account ID {account_id}: {e}")
        return f"Error: {e}"
    
def get_subaccounts(base_url):
    url = f"{base_url}/portfolio/subaccounts"
    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"An error occurred: {e}")
        return None

def summary_row(df):
    """Calculate and return a summary row for the given DataFrame."""
    summary = pd.DataFrame([{
        'Ticker': 'TOTAL',
        'Position': df['Position'].sum(),
        'mktValue': df['mktValue'].sum(),
        'PnL': df['PnL'].sum(),
        'avgCost': df['avgCost'].mean(),  # Averaging as an approximation
        'mktPrice': df['mktValue'].sum() / df['Position'].sum() if df['Position'].sum() != 0 else 0.0
    }])
    return summary
    
def aggregate_positions(positions_list):
    """Aggregate positions based on the Ticker, calculate the required fields, and add a summary row."""
    if not positions_list:
        return []

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(positions_list)
    
    # Ensure all relevant columns are present
    if 'Ticker' in df.columns:
        df['ticker'] = df['Ticker']
    elif 'ticker' not in df.columns:
        df['ticker'] = 'N/A'

    # Convert columns to numeric types where applicable
    df['Position'] = pd.to_numeric(df.get('Position', df.get('position', 0)), errors='coerce').fillna(0)
    df['mktValue'] = pd.to_numeric(df.get('mktValue', df.get('mktvalue', 0.0)), errors='coerce').fillna(0)
    df['PnL'] = pd.to_numeric(df.get('PnL', df.get('pnl', 0.0)), errors='coerce').fillna(0)
    df['avgCost'] = pd.to_numeric(df.get('avgCost', df.get('avgcost', 0.0)), errors='coerce').fillna(0)

    # Group by the 'ticker' and aggregate the values
    aggregated_df = df.groupby('ticker').agg({
        'Position': 'sum',
        'mktValue': 'sum',
        'PnL': 'sum',
        'avgCost': lambda x: (x * df.loc[x.index, 'Position']).sum() / df.loc[x.index, 'Position'].sum(),
        'mktPrice': lambda x: df.loc[x.index, 'mktValue'].sum() / df.loc[x.index, 'Position'].sum() if df.loc[x.index, 'Position'].sum() != 0 else 0.0
    }).reset_index()

    # Rename columns to match the expected format
    aggregated_df.rename(columns={'ticker': 'Ticker'}, inplace=True)

    return aggregated_df.to_dict('records')

def portfolios(account_id):
    """Retrieve and aggregate the portfolio details for all subaccounts."""
    try:
        port = gateway(account_id)
        base_url = f'https://localhost:{port}/v1/api'
        if not port:
            raise ValueError("Port retrieval failed. No valid port found.")

        subaccounts = get_subaccounts(base_url)
        if not subaccounts:
            return "Error: Unable to retrieve subaccounts list."

        all_positions = []

        for subaccount in subaccounts:
            account = subaccount['accountId']
            positions = get_positions(account, base_url)
            if positions:
                all_positions.extend(positions)

        if not all_positions:
            return "No positions found across all subaccounts."

        aggregated_positions = aggregate_positions(all_positions)
        if aggregated_positions is None:
            return "No positions found across all subaccounts."
        else:
            aggregated_df = pd.DataFrame(aggregated_positions)
            if len(aggregated_df) > 1:
                # Calculate summary values for the entire dataset
                # Concatenate the summary row to the aggregated DataFrame
                aggregated_df = pd.concat([aggregated_df, summary_row(aggregated_df)], ignore_index=True)
        
            return format_table(aggregated_df)

    except ValueError as ve:
        logger.error(f"Value error: {ve}")
        return f"Error: {ve}"
    except Exception as e:
        logger.error(f"Unexpected error while retrieving portfolio for account ID '{account_id}': {e}")
        return f"Error: {e}"


def summary(account_id):
    try:
        port = gateway(account_id)
        base_url = f'https://localhost:{port}/v1/api'

        if not port:
            raise ValueError("Port retrieval failed. No valid port found.")

        account = get_1password_secret(f"op://trade/{account_id}/account")
        
        url = f"{base_url}/portfolio/{account}/summary"
        if not account:
            return "Error: Unable to retrieve account."

        # Retrieve positions for the account
        positions_data = get_positions(account, base_url)

        try:
            response = requests.get(url, headers={'Content-Type': 'application/json'}, verify=False)
            response.raise_for_status()
            summary_data = response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred: {e}")
            return None

        if summary_data is None:
            return "No Portfolio Summary found for this account."
        else:
            summary_df = pd.DataFrame(summary_data)
            summary_df = summary_df.T
            summary_df = summary_df[['amount']]
            summary_df['amount'] = pd.to_numeric(summary_df['amount'], errors='coerce').fillna(0)
            summary_df = summary_df[summary_df['amount'] != 0]
            summary_df['amount'] = summary_df['amount'].apply(lambda x: f"{x:>20,.2f}")
            summary_df = summary_df[~summary_df.index.str.contains('-s')]

            # if len(positions_df) > 1:
            #     # Calculate summary values for the entire dataset
            #     # Concatenate the summary row to the aggregated DataFrame
            #     positions_df = pd.concat([positions_df, summary_row(positions_df)], ignore_index=True)
        
            # Format and return the positions data
            return (summary_df.to_string())

    except ValueError as ve:
        logger.error(f"Value error: {ve}")
        return f"Error: {ve}"
    except Exception as e:
        logger.error(f"Unexpected error while retrieving Portfolio Summary for account ID {account_id}: {e}")
        return f"Error: {e}"
    

def sell_all(account_id):
    """Dummy sell all function."""
    return ''

def main(args):
    if len(args) < 1:
        logger.info("No arguments provided. Showing all running Docker containers.")
        sys.exit(0)

    try:
        func_name = args[0]
        topic_id = args[2] if len(args) > 2 else None
        
        # Mapping the topic_id with account_id
        trading_group_secret = get_1password_secret("op://dev/Telegrambot/trading")
        trading_group = json.loads(trading_group_secret) if trading_group_secret else None        
        THREAD_ID_TO_TOPIC_NAME = {v: k for k, v in trading_group.items()}

        account_id = THREAD_ID_TO_TOPIC_NAME.get(str(topic_id), None)
        
        if func_name not in globals():
            raise AttributeError(f"Function '{func_name}' not found.")
        
        func = globals()[func_name]
        result = func(account_id)
                
        print(result)

    except AttributeError as ae:
        logger.error(f"AttributeError: {ae}")
        print(f"Error: {ae}")
    except Exception as ex:
        logger.error(f"Unexpected error: {ex}")
        print(f"Error: {ex}")

if __name__ == "__main__":
    main(sys.argv[1:])
