'''
The folder stock_market corresponds to the data pipeline
'''

import requests
import json
from airflow.hooks.base import BaseHook


def _get_stock_prices(url, symbol):

    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers = api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])