""" Top 106 tokens by marketcap 2025. """

import sys
from time import sleep
import json
import requests
import logging
import traceback
from pathlib import Path
from collections import defaultdict
from datetime import datetime, UTC
from concurrent.futures import (
	ProcessPoolExecutor, as_completed
)

logging.basicConfig(
	level=logging.DEBUG,
	format="%(asctime)s - %(levelname)s - %(message)s - [module -> %(filename)s] [func -> %(funcName)s]",
	datefmt="%Y:%m:%d %T",
	style="%",
	filename='err.log',
	filemode='a',
	encoding='utf-8'
)

class Log:
	def __init__(self):
		pass
		
	@staticmethod
	def get_error_detail():
		error_type, error_message, error_traceback = sys.exc_info()
		error_name = error_type.__name__
		frames = traceback.extract_tb(error_traceback)
		line_no = frames[-1].lineno
		return error_name, error_message, line_no
	
	@staticmethod
	def error():
		error_name, error_message, line_no = Log.get_error_detail()
		logging.error("%s - %s - [line %s]", error_name, error_message, line_no)
		
	@staticmethod
	def critical():
		error_name, error_message, line_no = Log.get_error_detail()
		logging.critical("%s - %s - [line %s]", error_name, error_message, line_no)
		
	@staticmethod
	def info(message):
		logging.info("%s", message)
				
class Api:
	def __init__(self, token):
		self.name = token[1].lower().strip()
		self.token_name = token[2].lower().strip()
		self.symbol = token[0].upper().strip()
		
	def get_data(self):
		url = f"https://api.coingecko.com/api/v3/coins/{self.token_name}/market_chart"
		params = {
			"vs_currency": "usd",
			"days": "365",
			"interval": "daily"
		}
		Log.info(f"Fetching 'API response' from -> '{url}'")
		time = 30
		for retries in range(5):
			response = requests.get(url, params=params)
			Log.info(f"'API response' retrieved. ['{self.name}' -> status code -> {response.status_code}]")
			if response.status_code == 200:
				Log.info(f"coverting '{self.name}' data to 'JSON format'")
				data = response.json()
				Log.info(f"'API data' -> '{self.name}' converted to 'JSON format'")
				sleep(60)
				return data					
			elif response.status_code == 429:
				time += 30
				sleep(time)
	
def task(token):
	try:
		asset_data = defaultdict(dict)
		asset = Api(token)		
		asset_symbol = asset.symbol
		asset_name= asset.name
		Log.info(f"asset object created for -> ['{asset_symbol}' -> '{asset_name}']")	
		currency = '$'
		data = asset.get_data()
		if not data:
			return None
		
		Log.info(f"sorting 'API data' -> '{asset_symbol}'")
		time_stamps = [datetime.fromtimestamp(content[0]/1000, UTC).strftime("%Y:%m:%d").strip() for content in data.get('prices',[])]
		prices = [content[1] for content in data.get('prices',[])]
		market_caps = [content[1] for content in data.get('market_caps',[])]
		volumes = [content[1] for content in data.get('total_volumes',[])]
		Log.info(f"'API data' sorted! -> '{asset_symbol}'")
		
		Log.info(f"creating database with sorted 'API data' -> '{asset_symbol}'")
		for time_stamp, price, market_cap, volume in zip(
			time_stamps, prices, market_caps, volumes
		):
			asset_data[time_stamp].update(
				{
					'currency': currency,
					'asset name': asset_name,
					'price': price,
					'market_cap': market_cap,
					'volume': volume
				}
			)
		Log.info("database created")			
		return asset_symbol, asset_data	
	except:
		Log.error()
		return None
	
def process_task(tokens):
	with ProcessPoolExecutor() as executor:
		tasks = [executor.submit(task, token) for token in tokens]
	
	for task_output in as_completed(tasks):
		if not task_output.result():
			continue
		asset_symbol, asset_data = task_output.result()
		Log.info(f"compiling database into -> 'global database' for -> '{asset_symbol}'")
		global_data[asset_symbol].update(asset_data)
	Log.info("database compilation completed!")
		
def save_data(dir, data):
	Log.info(f"attempting to save data in -> '{dir.parent}'")
	with dir.open('w', encoding='utf-8') as file:
		json.dump(
			data, 
			file, 
			ensure_ascii=False, 
			indent=4
		)
	if dir.is_file():
		Log.info(f"'{dir.name}' created! -> '{dir}'")
		return f"'{dir.name}' created! -> '{dir.parent}'"
	else:
		Log.info(f"Failed to save file -> '{dir.name}'!")
		return "Operation Failed!"
		
def get_file(dir):
	Log.info(f"attempting to retrieve '{dir.name}' from -> '{dir.parent}'")
	if dir.exists():
		with dir.open('r', encoding='utf-8') as f:
			data = json.load(f)
			Log.info(f"retrieved '{dir.name}'!")
			return data
	else:
		Log.info(f"'{dir.name}' does not exists in '{dir.parent}'")
		print(f"'{dir.name}' does not exists in '{dir.parent}'")
		
def merge_data(main_data, sub_data):
	if sub_data:		
		for key in sub_data.keys():
			if main_data.get(key): 
				main_data[key].update(sub_data.get(key))
				continue
			main_data[key] = sub_data[key]
		message = save_data(main_file, main_data)
		return message
	return "'sub_data' is empty!"

if __name__ == '__main__':
	try:	
		global_data = defaultdict(dict)		
		global_data_structure = (
"""
	global_data = {
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
"""
        )
		Log.info(f"declared 'global_data' variable -> {global_data_structure}")
		
		api_data_structure = (
"""
    api_data = {
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
		
"""
        )
		Log.info(f"'API data' structure -> {api_data_structure}")		
	    
		sub_file = Path('sub_dataset.json')
		main_file = Path('main_dataset.json')
		tokens_dir = Path('tokens.json')
		tokens = get_file(tokens_dir)
		main_data = get_file(main_file)
		process_task(tokens)
		message = save_data(sub_file, global_data)
		message_x = merge_data(main_data, global_data)
		print(
f"""API operation status -> {message} <-
Merge file operation status -> {message_x} <-"""
        )
		info = f"""global_data keys -> {len(global_data.keys())}: main_data keys -> {len(main_data.keys())}"""
		Log.info(info)		
	except:
		Log.critical()

	

	