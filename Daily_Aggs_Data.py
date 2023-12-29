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
directory_path = r'C:\Users\Random\Documents\DataCollector\1D_Aggs_Daa'

#Parameter Variables
multiplier = 1
timespan = 'day'
start_date = datetime.date(2019, 12, 5)
end_date = datetime.date(2023, 12, 1)


def fetch_data_in_chunks(start_date, end_date, symbol, multiplier, timespan):
    data = pd.DataFrame()
    current_start_date = start_date

    while current_start_date < end_date:
        # Calculate end date for this chunk
        days_per_request = 100  # 64 aggregates per day including pre/post market
        current_end_date = min(current_start_date + datetime.timedelta(days=days_per_request), end_date)
        # Fetch data for the current chunk
        chunk_data = client.get_aggs(
            symbol,
            multiplier,
            timespan,
            current_start_date.strftime('%Y-%m-%d'),
            current_end_date.strftime('%Y-%m-%d'),
            adjusted=False,
            sort='asc',
            limit=50000,
            raw=False
        )
        #print(chunk_data)
        # Append or concatenate the chunk data to the main data list
        chunk_df = pd.DataFrame(chunk_data)
        data = pd.concat([data, chunk_df], ignore_index=True)
        # Update the start date for the next chunk
        current_start_date = current_end_date + datetime.timedelta(days=1)
  
    return data
for symbol in symbols:
    agg_data = fetch_data_in_chunks(start_date, end_date, symbol, multiplier, timespan)
    file_path = os.path.join(directory_path, str(symbol) + '_1D_aggs_data.json')
    agg_data.to_json(file_path, orient='records', indent=4)
    print(str(symbol) + ' Data Saved')


{
    "results": [
        {
            "id": "6AaNYn329cpVgNvb9EEblDaNi0QTnw1YUDElTUjH208",
            "publisher": {
                "name": "The Motley Fool",
                "homepage_url": "https://www.fool.com/",
                "logo_url": "https://s3.polygon.io/public/assets/news/logos/themotleyfool.svg",
                "favicon_url": "https://s3.polygon.io/public/assets/news/favicons/themotleyfool.ico"
            },
            "title": "Invest Like Warren Buffett, Not Carl Icahn",
            "author": "newsfeedback@fool.com (Adam Levine-Weinberg)",
            "published_utc": "2017-04-10T00:24:00Z",
            "article_url": "https://www.fool.com/investing/2017/04/09/invest-like-warren-buffett-not-carl-icahn.aspx",
            "tickers": [
                "BRK.B",
                "IEP",
                "BRK.A",
                "AAPL",
                "NFLX"
            ],
            "image_url": "https://g.foolcdn.com/editorial/images/435736/warren-buffett3_tmf.jpg",
            "description": "Warren Buffett and Carl Icahn are two of the most successful investors of the past century. But Buffett is a superior model for investors to follow because of his patient style and focus on finding great businesses.",
            "keywords": [
                "investing"
            ]
        }
    ]
}