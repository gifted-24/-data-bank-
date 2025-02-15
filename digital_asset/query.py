"""Query.
NOTE:
    1. This script is designed specifically to query the dataset called 'dataset.json'.
	
	2. It requires the following inputs from the user:
	    A. The 'token symbol' e.g 'BTC', 'ETH' etc.
		B. The Time range for historical data:
		    
			--/--/-- to --/--/--
			YYYY/MM/DD  YYYY/MM/DD
		C. Be certain To check the Last recorded Date.
		
AUTHOR: Esefo Gift
GMAIL: giftesefo78@gmail.com
"""

from collections import defaultdict
from file_io import (
	database_dir, get_file, save_file, Path
)
from tools import query_database
from log import log

if __name__ == '__main__':
	try:
		print(
"""NOTE:
    1. The dataset begins from: '2024/02/17'
"""
        ) 
		database = get_file(database_dir)	
		token_data = defaultdict(dict)
		token_symbol = input(
			'Token Symbol: '
		).strip().upper()
		query_database(
			database, token_data, token_symbol
		)
		token_dir = Path(
			f'json_file/{token_symbol.lower()}.json'
		)
		save_file(
			token_dir, token_data
		)
	except:
		log.critical() 
    
    