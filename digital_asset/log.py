"""Log module.

This module contains a class and the class object called 'log'.
To use this module simply import the object called 'log' in the
script you intend on using it in.

>>> python_file.py
... from import log 

This gives you access to methods such as:
>>> log.error()
... log.critical()
... log.info()

NOTE:
    1. To customize the file where you want to log this simply edit
    the string in the variable called 'file_name'.
    >>> file_name = Path('custom-file-name')

    2. For this module to work perfectly it must be in the same directory
    with the files you are using it in.

    3. The scripts you'll be executing using this will all log to the same 
    'log fle' if in the same diectory.  

AUTHOR: Esefo Gift 
GMAIL: giftesefo78@gmail.com    
"""

import logging
import traceback
import sys
from pathlib import Path

Path('log').mkdir(parents=True, exist_ok=True)
file_name = Path('console.log')
logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s - %(levelname)s - %(message)s",
	datefmt="%Y:%m:%d %T",
	style="%",
	filename=f'log/{file_name}',
	filemode='a',
	encoding='utf-8'
)

class Log:
    def __init__(self):
        pass
		
    def get_error_detail(self):
        error_type, error_message, error_traceback = sys.exc_info()
        error_name = error_type.__name__
        frames = traceback.extract_tb(error_traceback)
        frame = frames[-1]
        line_no = frame.lineno
        file_name = Path(frame.filename).name
        return file_name, error_name, error_message, line_no
	
    def error(self):
        file_name, error_name, error_message, line_no = self.get_error_detail()
        logging.error("%s - %s - ['%s' -> line %s]", error_name, error_message, file_name, line_no)
		
    def critical(self):
        file_name, error_name, error_message, line_no = self.get_error_detail()
        logging.critical("%s - %s - ['%s' -> line %s]", error_name, error_message, file_name, line_no)
		
    def info(self, message):
        logging.info("%s", message)

log = Log()
