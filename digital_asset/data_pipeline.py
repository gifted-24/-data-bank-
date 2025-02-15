"""Data pipeline.

This script is a data pipeline meant to fetch, process, clean and restructure 
'historical data' of the 'Top 100+ crypto-currencies by market-cap'.

Below are the database structure from 'coingecko-API' and the cleaned database:

coingecko-API:
    
	{
		prices: [
			time_stamp <- float,
			price <- float
		],
		market_caps: [
			time_stamp <- float,
			market_cap <- float
		],
		total_volumes: [
			timestamp <- float,
			volume <- float
		]
	}

cleaned database:

	{
		asset_symbol: {
			time_stamp <- str: {
				currency: <- str,
				price: <- float,
				market_cap: <- float,
				volume: <- float
			}
		},
		...,
	}

AUTHOR: Esefo Gift 
GMAIL: giftesefo78@gmail.com	
"""

from file_io import (
	save_file, get_file, tokens_dir, database_dir
)
from log import log
from tools import (
	process_task, match, batch_tokens
)
from time import sleep
from collections import defaultdict
import json

if __name__ == '__main__':
	try:	
		database_cache = defaultdict(dict)
		if database_dir.is_file():
			database = get_file(database_dir)
			database = defaultdict(dict, database)
		else:
			database = database_cache				
		log.info(f"declared 'database_cache' variable -> {database_cache}")
				
		#tokens = get_file(tokens_dir)
		tokens = [
			[
				"BNB",
				"binance coin",
				"binancecoin"
			]
		]
		status = dict()
		status['retries'] = 0

		missing_tokens = match(database_cache, tokens).get('missing tokens')
		while missing_tokens:
			batches = batch_tokens(tokens, 5)
			for token_batch, tag in zip(batches, range(1, (len(missing_tokens) + 1))):
				log.info(f"Fetching historical data: 'batch[{tag}]' - {[token[0] for token in token_batch]}")
				process_task(token_batch, database, database_cache)
				log.info(f"database compiled for -> 'batch[{tag}]'")
				save_file(database_dir, database)
				sleep(60)
			missing_tokens = match(database_cache, tokens).get('missing tokens')		
			status.update(
					{
						'Missing Tokens': missing_tokens,
						'Available Tokens': len(database_cache.keys()),
						'coingecko API request-status': 'successful ✨'
					}
			)
			if missing_tokens:
				status['retries'] += 1
				log.info(status)
			if status['retries'] == 10:
				break
		status = json.dumps(
			status,
			indent=4,
			ensure_ascii=False
		)
		log.info(status)		 
		print(status)
	except:
		log.critical()
