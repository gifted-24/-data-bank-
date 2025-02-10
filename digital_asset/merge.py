from asset_dataset_pipeline import (
	save_data, get_file
)
from pathlib import Path

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
	main_file = Path('main_dataset.json')
	sub_file = Path('sub_dataset.json')
	main_data = get_file(main_file)
	sub_data = get_file(sub_file)
	message = merge_data(main_data, sub_data)
	print(f"{message}: main_data keys -> {len(main_data.keys())}") 
