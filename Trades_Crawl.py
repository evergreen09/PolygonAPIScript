from polygon import RESTClient
import requests
import json
from dotenv import load_dotenv
import os
import datetime
import pandas as pd

load_dotenv()

api_key = os.getenv('POLYGON_API_KEY')
client = RESTClient(api_key)
end_date = datetime.date(2016, 9, 3)
symbol = 'AAPL'
timestamp=datetime.date(2016, 9, 1)


def get_trades(ticker, timestamp):
    data = pd.DataFrame()
    file_path = f'{ticker}_{timestamp}.json'
    counter = 0
    trades = client.list_trades(ticker=ticker,
                                timestamp=timestamp.strftime('%Y-%m-%d'),
                                limit=50000,
                                sort='asc',
                                raw=False)
    for trade in trades:
        counter += 1
        trade_dic = trade.__dict__
        trade_dict_list = {k: [v] for k, v in trade_dic.items()}
        trade_data = pd.DataFrame(trade_dict_list)
        data = pd.concat([data, trade_data], ignore_index=True)
        # Monitor progress
        if counter % 1000 == 0:  # Print a message every 100 records
            print(f"Processed {counter} records")
    data.to_json(file_path, orient='records', indent=4)

def sort_json_by_timestamp(file_path):
    # Load the JSON file
    with open(file_path, 'r') as file:
        data = json.load(file)

    # Sort the data by 'timestamp' key
    sorted_data = sorted(data, key=lambda x: x['sip_timestamp'])

    # Save the sorted data back to a JSON file  
    with open(file_path, 'w') as file:
        json.dump(sorted_data, file, indent=4)


while timestamp < end_date:
    file_path = f'{symbol}_{timestamp}.json'
    get_trades(symbol, timestamp)
    sort_json_by_timestamp(file_path)
    timestamp = timestamp + datetime.timedelta(days=1)
    if timestamp.weekday == 5:
        timestamp = timestamp + datetime.timedelta(days=2)
    elif timestamp == 6:
        timestamp = timestamp + datetime.timedelta(days=1)
    




    
