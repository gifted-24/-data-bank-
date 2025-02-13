"""Coingecko API.

This is used to access the 'coingeck-API' to fetch historical data on crypto-currencies dated back to 
a limited time period.

NOTE: 
	1. The max period available to 'non - premium users' is '365 days' aand anything above this will
	result in an ERROR except you use a premium account.
	
	2. you don't need to have an account with 'coingecko' before you can make use of this API.

	3. The maximum number of 'request per minute' is 5 and anything above this will return a '429' status
	code.

	4. The status code '429', '404' and '200' means 'max entries exceeded', 'Not Found' and 'request was successful' respectively
	as you'll find in the 'log file'.

	5. To get more info on how to utilize the 'coingecko-API' you can access their official platform.

AUTHOR: Esefo Gift 
GMAIL: giftesefo78@gmail.com
"""

import requests
from time import sleep
from log import log

class Api:
	def __init__(self, token):
		self._name = token[2].lower().strip()
		self.symbol = token[0].upper().strip()
		self.name = token[1].lower().strip()
		
	def get_data(self):
		url = f"https://api.coingecko.com/api/v3/coins/{self._name}/market_chart"
		params = {
			"vs_currency": "usd",
			"days": "365",
			"interval": "daily"
		}
		log.info(f"Fetching 'API response' from -> '{url}'")
		time = 60
		for retries in range(3):
			response = requests.get(url, params=params)
			log.info(f"'API response' retrieved. ['{self.symbol}' -> status code: {response.status_code}]")
			if response.status_code == 200:
				log.info(f"coverting '{self.symbol}' data to 'JSON format'")
				api_data = response.json()
				log.info(f"'API data' -> '{self.symbol}' converted to 'JSON format'")
				return api_data					
			elif response.status_code == 429:
				sleep(time)
				time += 60
			elif response.status_code == 404:
				continue
			else: 
				return None
	