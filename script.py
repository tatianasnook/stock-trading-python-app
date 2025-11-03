import os
import time
import csv
import requests
from dotenv import load_dotenv


load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000


def run_stock_job():
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"
    response = requests.get(url)
    tickers = []

    data = response.json()

    for ticker in data['results']:
        tickers.append(ticker)

    # data load by chuncks by 1000 per page. to load next page we have to use a while loop:
    while 'next_url' in data:
        print('requesting next page', data['next_url'])
        time.sleep(15)
        response = requests.get(
            data['next_url'] + f"&apiKey={POLYGON_API_KEY}")
        data = response.json()
        print(data)
        # print(data.keys())
        for ticker in data['results']:
            tickers.append(ticker)

    # print(len(tickers))

    example_ticker = {
        'ticker': 'HOV',
        'name': 'Hovnanian Enterprises, Inc. Class A',
        'market': 'stocks',
        'locale': 'us',
        'primary_exchange': 'XNYS',
        'type': 'CS',
        'active': True,
        'currency_name': 'usd',
        'cik': '0000357294',
        'composite_figi': 'BBG000BLCBN7',
        'share_class_figi': 'BBG001S5RZM4',
        'last_updated_utc': '2025-10-27T06:05:54.102024151Z'}

    # Write results to CSV with the same schema (columns and order) as example_ticker
    output_csv = "tickers.csv"
    fieldnames = list(example_ticker.keys())

    with open(output_csv, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for t in tickers:
            row = {key: t.get(key, '') for key in fieldnames}
            writer.writerow(row)

    print(f"Wrote {len(tickers)} rows to {output_csv}")


if __name__ == '__main__':
    run_stock_job()
