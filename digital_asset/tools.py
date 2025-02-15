"""Tools module.

NOTE:
	1. This module contains a function needed for parrallel computing or 
    concurrent execution of requests to the 'coingecko-API' -> 'process_task'.
    
    2. It also contains other functions needed for the 'data_pipeline.py'
    script.
    
AUTHOR: Esefo Gift 
GMAIL: giftesefo78@gmail.com    
"""

from concurrent.futures import (
    ProcessPoolExecutor, as_completed
)
from datetime import (
    datetime, UTC
)
from log import log
from coingecko import Api
import requests
from time import sleep
from collections import defaultdict
from copy import deepcopy

__all__ = [
      'process_task',
      'task',
      'batch_tokens',
      'count',
      'match'
]

def process_task(tokens, database, database_cache):
	try:
		with ProcessPoolExecutor() as executor:
			tasks = [executor.submit(task, token) for token in tokens] 
		for task_output in as_completed(tasks):
			if not task_output.result():
				continue
			asset_symbol, asset_data = task_output.result()
			log.info(f"compiling database: '{asset_symbol}'")
			database[asset_symbol].update(asset_data)
			database_cache[asset_symbol].update(asset_data)
	except:
		log.error()

def task(token):
	try:
		asset_data = defaultdict(dict)
		asset = Api(token)		
		asset_symbol = asset.symbol
		asset_name= asset.name
		log.info(f"API object created: ['{asset_symbol}' -> '{asset_name}']")	
		currency = '$'
		api_data = asset.get_data()
		if not api_data:
			return None
		
		log.info(f"sorting 'API data' -> '{asset_symbol}'")
		time_stamps = [datetime.fromtimestamp(content[0]/1000, UTC).strftime("%Y:%m:%d").strip() for content in api_data.get('prices', [])]
		prices = [content[1] for content in api_data.get('prices', [])]
		market_caps = [content[1] for content in api_data.get('market_caps', [])]
		volumes = [content[1] for content in api_data.get('total_volumes', [])]
		log.info(f"'API data' sorted -> '{asset_symbol}'")
		
		log.info(f"creating database with sorted 'API data': '{asset_symbol}'")
		for time_stamp, price, market_cap, volume in zip(
			time_stamps, prices, market_caps, volumes
		):
			asset_data[time_stamp].update(
				{
					'currency': currency,
					'asset name': asset_name,
					'price': price,
					'market cap': market_cap,
					'volume': volume
				}
			)
		log.info(f"database created for -> '{asset_symbol}'")			
		return asset_symbol, asset_data	
	except:
		log.error()
		return None
	
def batch_tokens(tokens, per_batch=10):
    try:
        for batch_range in range(0, len(tokens), per_batch):
            token_batch = tokens[batch_range: (per_batch + batch_range)]
            yield token_batch
    except:
        log.error()
		
def count(database, tokens):
    try:
        database_key_count = len(database.keys())
        token_count = len(tokens)
        return token_count, database_key_count
    except:
        log.error()
    
def match(database, tokens):
    try:
        token_count, database_key_count = count(database, tokens)
        result = defaultdict(list)
        if (token_count > database_key_count) and database.keys():
            for key in database.keys():
                remove_tokens = [tokens.remove(token) for token in tokens if token[0] == key]
            result['missing tokens'].extend(tokens)
            result['no. of missing tokens'] = len(tokens)
        elif not database.keys():
            result['missing tokens'] = tokens
        elif token_count == database_key_count:
            result['missing tokens'] = 0
        return result
    except:
        log.error()

def is_leap_year(year):
	if (year % 4 == 0) or (year % 100 == 0 and year % 400 == 0):
		return True
	else:
		return False

def entries():
	try:
		from_year = int(
			input(
				'From Year: '
			).strip()
		)
		from_month = int(
			input(
				f"From Month: {from_year}/" 
			).strip()
		)
		from_day = int(
			input(
				f"From Day: {from_year}/{from_month}/"
			).strip()
		)
		to_year = int(
			input(
				'To Year: '
			).strip()
		)
		to_month = int(
			input(
				f"To Month: {to_year}/"
			).strip()
		)
		up_to = int(
			input(
				f"Up To: {to_year}/{to_month}/"
			).strip()
		)
		return from_year, from_month, from_day, to_year, to_month, up_to	
	except:
		log.error()

def query_database(
	database, token_data, token_symbol
):
	try:
		from_year, from_month, from_day, to_year, to_month, up_to = entries()
		
		is_leap_year_cache = set()
		is_not_leap_year_cache = set()
		for year in range(from_year, (to_year + 1)):
			if (year not in is_leap_year_cache) and (year not in is_not_leap_year_cache):
				if is_leap_year(year):
					is_leap_year_cache.add(year)
					months['february'] = 29
				else:
					is_not_leap_year_cache.add(year)
					months['february'] = 28
			for month, month_no in zip(months.keys(), range(1, 13)):
				if (year == from_year and month_no < from_month) or (year == to_year and month_no > to_month):
					continue
				if (month_no < 10):
					month_no = f'0{month_no}'
				if (year == from_year) and (int(month_no) == from_month):
					start = from_day
				else:
					start = 1
					
				days = months[month]		
				for day in range(start, (days + 1)): 
					if (year == to_year) and (int(month_no) == to_month and day > up_to):
						break
					if (day < 10):
						day = f'0{day}'
					time_stamp = f'{year}:{month_no}:{day}'
					token_data[time_stamp].update(database[token_symbol].get(time_stamp))
	except KeyError:
		raise KeyError(f"Invalid Entry: '{token_symbol}': '{time_stamp}'")
	except:
		log.error()


months = {
	'January': 31, 
	'february': [28, 29],
	'march': 31,
	'april': 30,
	'may': 31,
	'june': 30,
	'july': 31,
	'august': 31,
	'september': 30,
	'october': 31,
	'november': 30,
	'december': 31
}
