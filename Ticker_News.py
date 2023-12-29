import requests
import json
from dotenv import load_dotenv
from polygon import RESTClient
import os
import datetime
import pandas as pd

load_dotenv()

api_key = os.getenv('POLYGON_API_KEY')
client = RESTClient(api_key)

file_path = 'stocks_list.csv'
df = pd.read_csv(file_path)
symbols = df['Symbol']
directory_path = r'C:\Users\Random\Documents\DataCollector\Ticker_News_Data'
#directory_path = r'/Users/ltk/Documents/DataCollector/Ticker_News_Data'

for symbol in symbols:
    num = 1
    file_path = os.path.join(directory_path, str(symbol) + '_Ticker_News_Data.json')
    response = client.list_ticker_news(
        ticker=symbol,
        limit=1000,
        sort='published_utc',
        order='asc',
        raw=True
    )
    response_data = json.loads(response.data.decode('utf-8'))
    new_data = response_data
    while new_data.get('next_url', None) != None:
        name = 'result_' + str(num)
        url = new_data['next_url']
        headers = {
            'Authorization': 'Bearer FCnZkTmpXx19B7bOugVQC642fSlt82Ov'
        }
        response = requests.get(url, headers=headers)
        new_data = response.json()
        new_data[name] = new_data.pop('results')
        response_data.update(new_data)
        print(num)
        num+=1
    with open(file_path, 'w') as file:
        json.dump(response_data, file, indent=4)
    print(symbol + ' Saved')
