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
	save_file, get_file, update_file, tokens_dir, database_dir
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
		database = defaultdict(dict)				
		log.info(f"declared 'database' variable -> {database}")
				
		tokens = get_file(tokens_dir)
		status = dict()
		status['retries'] = 0

		missing_tokens = match(database, tokens).get('missing tokens')
		while missing_tokens:
			batches = batch_tokens(tokens, 5)
			for token_batch, tag in zip(batches, range(1, (len(missing_tokens) + 1))):
				log.info(f"Fetching historical data: 'batch[{tag}]' - {[token[0] for token in token_batch]}")
				process_task(token_batch, database)
				log.info(f"database compiled for -> 'batch[{tag}]'")
				sleep(60)
			status['retries'] += 1
			missing_tokens = match(database, tokens).get('missing tokens')		
			status.update(
					{
						'missing tokens': missing_tokens,
						'database tokens': len(database.keys()),
						'coingecko API request-status': 'successful ✨'
					}
			)
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
		if database_dir.exists():
			database = update_file(database_dir, database)
		message = save_file(database_dir, database)
		print(message)	
	except:
		log.critical()
