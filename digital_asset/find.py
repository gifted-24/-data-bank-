from tools import match, count
from file_io import database_dir, get_file, tokens_dir


database = get_file(database_dir)
tokens = get_file(tokens_dir)

missing_tokens = match(database, tokens).get('missing tokens')

print(missing_tokens)