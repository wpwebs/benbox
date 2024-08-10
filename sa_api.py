import requests
from datetime import datetime
import sqlite3
import subprocess

# Function to retrieve the API key using the 1password CLI
def get_api_key_from_1password(op_reference):
    try:
        # Call the `op` command to read the API key from 1Password
        result = subprocess.run(['op', 'read', op_reference], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        api_key = result.stdout.strip()
        return api_key
    except subprocess.CalledProcessError as e:
        print(f"Error reading API key from 1Password: {e.stderr}")
        return None

def load_dict_from_json_file(file_path):
    import json
    with open(file_path, 'r') as file:
        # Read and parse the JSON content of the file
        dict_list = json.load(file)
    
    return dict_list

# Function to save data to the database
def save_to_db(cursor, table, date, tickers):
    # Create table if it does not exist
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS "{table}" (
            date TEXT,
            ticker TEXT
        )
    ''')
    data = [(date, ticker) for ticker in tickers]
    cursor.executemany(f'INSERT INTO "{table}" (date, ticker) VALUES (?, ?)', data)

# # Function to get the list of screeners
# def get_screeners(headers):
#     URL = "https://seeking-alpha.p.rapidapi.com/screeners/list"
#     response = requests.get(URL, headers=headers)
#     response.raise_for_status()
#     return response.json().get('data', [])

# # Function to get tickers from a screener
# def get_tickers(headers, screeners_list, screener_name):
#     URL = "https://seeking-alpha.p.rapidapi.com/screeners/get-results"
#     QUERYSTRING = {"page": "1", "per_page": "100", "type": "stock"}
#     for screener in screeners_list:
#         screen_name = screener['attributes']['name']
#         if screen_name == screener_name:
#             screen_filters = screener['attributes']['filters']
#             response_results = requests.post(URL, json=screen_filters, headers=headers, params=QUERYSTRING)
#             response_results.raise_for_status()
#             return [item['attributes']['name'] for item in response_results.json().get('data', [])]
#     return []

# Function to get tickers from a screener
def get_tickers(headers, filters):
    URL = "https://seeking-alpha.p.rapidapi.com/screeners/get-results"
    QUERYSTRING = {"page": "1", "per_page": "100", "type": "stock"}
    response_results = requests.post(URL, json=filters, headers=headers, params=QUERYSTRING)
    response_results.raise_for_status()
    return [item['attributes']['name'] for item in response_results.json().get('data', [])]

def main():
    # Connect to the SQLite database
    conn = sqlite3.connect('sa_4.db')
    cursor = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')
    
    headers = {
        "x-rapidapi-key": get_api_key_from_1password("op://dev/rapidapi/API_KEY"),
        "x-rapidapi-host": "seeking-alpha.p.rapidapi.com"
    }

    if not headers["x-rapidapi-key"]:
        print("API key retrieval failed. Exiting.")
        return

    file_path = 'filters.json'  # Path to your JSON file
    dict_list = load_dict_from_json_file(file_path)
    filters_names = []
    for i, dictionary in enumerate(dict_list):
        filters_name = dictionary.get('name')
        filters = dictionary.get('filters')
        tickers = get_tickers(headers, filters)       
        
        if tickers:
            save_to_db(cursor, filters_name, today, tickers)
            
        filters_names.append(filters_name)
    
        print(f"\nFilter name: {filters_name}")
        print(f"Number of tickers: {len(tickers)}")
        print(f"Tickers: {tickers}")
    
    # Commit and close the database connection
    conn.commit()
    conn.close()

if __name__ == '__main__':
    main()
