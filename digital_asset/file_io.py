"""File io.
NOTE:
    1. This script contains functions suited for 'file io' tasks.

    2. This module is designed specifically for the file 'data_pipeline.py'.

    3. If you intend on using the 'save_file' and 'get_file' function across
    other scripts its advise you edit out from 'save_file' this lines:
    >>> if dir.exists():
    ...     data = update_file(dir, data)

AUTHOR: Esefo Gift 
GMAIL: giftesefo78@gmail.com
"""

from pathlib import Path
import json
from log import log

__all__ = [
    'update_file',
    'save_file',
    'get_file',
    'database_dir',
    'tokens_dir'
]

def update_file(database_dir, database):
    try:
        log.info(f"updating database with recent data -> '{database_dir}'")
        old_database = get_file(database_dir)
        old_database.update(database)
        return old_database
    except:
        log.error()
	
def save_file(dir, data):
    try:
        log.info(f"saving data to -> '{dir}'")
        with dir.open('w', encoding='utf-8') as file:
            json.dump(
                data, 
                file, 
                ensure_ascii=False, 
                indent=4
            )
        if dir.is_file():
            log.info(f"'{dir.name}' created! -> '{dir}'")
        else:
            log.info(f"Failed to save file -> '{dir.name}'!")
    except:
        log.error()

def get_file(dir):
    try:
        log.info(f"attempting to retrieve '{dir.name}' from -> '{dir.parent}'")
        if dir.exists():
            with dir.open('r', encoding='utf-8') as f:
                data = json.load(f)
                log.info(f"retrieved '{dir.name}'!")
                return data
        else:
            log.info(f"'{dir.name}' does not exist in '{dir.parent}'")
    except:
        log.error()

database_dir = Path('json_file/dataset.json')
tokens_dir = Path('json_file/tokens.json')
