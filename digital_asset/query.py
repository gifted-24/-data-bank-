from collections import defaultdict
from file_io import (
	database_dir, get_file, save_file, Path
)
from tools import query_database

if __name__ == '__main__':
    database = get_file(database_dir)	
    token_data = defaultdict(dict)
    
    token_name = input(
        'Token name: '
    ).strip().upper()
    
    query_database(
        database, token_data, token_name
    )
    token_dir = Path(
        f'json_file/{token_name.lower()}.json'
    )
    save_file(
        token_dir, token_data
    ) 
    
    