import os
from datetime import datetime as dt
from dateutil import parser
import re

""" 
...
	project
		[user]
			key1
				summary
					[year]
						[month]
							[day]
								[yyyy-mm-dd hh:mm:ss]
									[count up to that point]
				[year]
					[month]
						[date]
							[yyyy-mm-dd hh:mm:ss]
								[count]
			key2
				...
"""

BATCH_KEY = "summary"
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y/%m/%d"

### HELPERS
class NoYearHandlingParserInfo(parser.parserinfo):
	yearFirst=True
	def convertyear(self, year, *args, **kwargs):
		""" dateutils.parser.parse parses 1 as 2001. This corrects it to parse 1 as in 1AD """
		return int(year)

class RecordFilter:
	BEFORE = 0
	BEFORE_OR_EQUAL = 1
	AFTER = 2
	AFTER_OF_EQUAL = 3
	VALID = set([BEFORE, BEFORE_OR_EQUAL, AFTER, AFTER_OF_EQUAL])
	parser_info = NoYearHandlingParserInfo(yearfirst=True)

def time_to_date_path(time: dt):
	""" Converts datetime object to folder path based on PATH_FORMAT """
	return time.strftime(DATE_FORMAT)

def time_to_time_dir(time: dt):
	""" Converts datetime object to file name based on FILE_FORMAT """
	return time.strftime(TIME_FORMAT)
	
def file_to_time(p: str):
	"""" Converts a file or path into a datetime object """
	try:
		f = os.path.basename(p)
		return parser.parse(f, parserinfo=RecordFilter.parser_info, ignoretz=True)
	except TypeError:
		return None

def get_batch_key(key: str):
	""" Converts a key into a batch key """
	return os.path.join(key, BATCH_KEY)

def get_int_dirs(path: str):
	""" Returns directories in a path whose names are ints """
	if os.path.isdir(path):
		return [f for f in os.listdir(path) if os.path.isdir(os.path.join(path, f)) and f.isdigit()]
	return []

def get_latest_in_path(path: str):
	""" Returns max file in a path by its name """
	if os.path.isdir(path):
		files = [f for f in os.listdir(path)]
		if len(files) > 0:
			return max(files)

def get_regex_group(pattern: str, string: str, flags: re.RegexFlag = 0):
	""" Returns first matching group given regex pattern and string or None if no match """
	match = re.search(pattern, string, flags)
	if match:
		return match.group()

### RECORDS
def add_record(path: str, user: str, key: str, time: dt, val: int = 1):
	""" Adds record for user and key sharded on time """
	date_path = time_to_date_path(time)
	time_path = time_to_time_dir(time)
	final_dir = os.path.join(path, user, key, date_path, time_path)	
	if not os.path.exists(final_dir):
		os.makedirs(final_dir)
	final_path = os.path.join(final_dir, "{}".format(val))
	with open(final_path, "w") as f:
		f.write("")
	return final_path

def get_records_filtered(path: str, user: str, key: str, time: dt, f: RecordFilter):
	""" Returns the number of records after a given time """
	filters = [lambda d: d == user, lambda d: d == key]
	time_components = time.strftime(DATE_FORMAT).split("/")

	if f not in RecordFilter.VALID:
		raise ValueError("Invalid record filter: {}. Valid filters: {}".format(f, RecordFilter.VALID))

	for t in time_components:
		if f in (RecordFilter.BEFORE, RecordFilter.BEFORE_OR_EQUAL):
			path_filter = lambda d: d.isdigit() and int(d) <= int(t)
		elif f in (RecordFilter.AFTER, RecordFilter.AFTER_OF_EQUAL):
			path_filter = lambda d: d.isdigit() and int(d) >= int(t)
		filters.append(path_filter)

	if f == RecordFilter.BEFORE:
		file_filter = lambda f: file_to_time(f) and file_to_time(f) < time
	elif f == RecordFilter.BEFORE_OR_EQUAL:
		file_filter = lambda f: file_to_time(f) and file_to_time(f) <= time
	elif f == RecordFilter.AFTER:
		file_filter = lambda f: file_to_time(f) and file_to_time(f) > time
	elif f == RecordFilter.AFTER_OF_EQUAL:
		file_filter = lambda f: file_to_time(f) and file_to_time(f) >= time

	filters.append(file_filter)

	paths = [path]
	for f in filters:
		new_paths = []
		for p in paths:
			new_dirs = [d for d in os.listdir(p) if f(d)]
			for n in new_dirs:
				new_paths.append(os.path.join(p, n))
		paths = new_paths
	num_files = 0
	for p in paths:
		num_files += sum([int(f) for f in os.listdir(p) if f.isdigit()])
	return num_files

def get_latest_record(path: str, user: str, key: str, val: bool = False):
	""" Returns latest record """
	cur_path = os.path.join(path, user, key)
	cur_sub_dirs = get_int_dirs(cur_path)
	while len(cur_sub_dirs) > 0:
		new_dir = max(cur_sub_dirs, key=lambda x: int(x))
		cur_path = os.path.join(cur_path, new_dir)
		cur_sub_dirs = get_int_dirs(cur_path)
	latest_file = get_latest_in_path(cur_path)
	if latest_file:
		latest_path = os.path.join(cur_path, latest_file)
		if val:
			latest_file_val = sum([int(f) for f in os.listdir(latest_path) if f.isdigit()])
			return latest_file_val
		return latest_path

def get_latest_record_time(path: str, user: str, key: str):
	""" Returns time of lastest record """
	latest_record = get_latest_record(path, user, key, val=False)
	return file_to_time(latest_record)

### BATCHING
def batch(path: str, user: str, key: str):
	""" Batches record for given path, user and key """
	records_to_batch = get_records_filtered(path, user, key, dt.min, RecordFilter.AFTER_OF_EQUAL)
	return add_summary(path, user, key, dt.now(), records_to_batch)

def batch_all_keys(path: str, user: str):
	""" Batches all records for given path and user"""
	user_path = os.path.join(path, user)
	keys = os.listdir(user_path)
	return [batch(path, user, k) for k in keys]

def batch_all_users(path: str):
	""" Batches all records for given path"""
	users = os.listdir(path)
	batch_summaries = []
	for user in users:
		batch_summaries.append(batch_all_keys(path, user))
	return batch_summaries

def get_latest_batch(path: str, user: str, key: str, val: bool = False):
	""" Returns latest batch result """
	new_key = get_batch_key(key)
	return get_latest_record(path, user, new_key, val)

def get_latest_batch_time(path: str, user: str, key: str):
	""" Returns time of last batch """
	latest_batch = get_latest_batch(path, user, key, val=False)
	return file_to_time(latest_batch)

def add_summary(path: str, user: str, key: str, time: dt, val: int):
	""" Adds a summary record for user and key sharded on time """
	new_key = get_batch_key(key)
	val = "{}".format(val)
	return add_record(path, user, new_key, time, val)

### SPEED
def update_batch(path: str, user: str, key: str):
	""" Updates summary without going through all records"""
	latest_batch_val = get_latest_batch(path, user, key, val=True)
	latest_batch_time = get_latest_batch_time(path, user, key)
	records_to_update = get_records_filtered(path, user, key, latest_batch_time, RecordFilter.AFTER_OF_EQUAL)
	update_val = int(latest_batch_val) + records_to_update
	return add_summary(path, user, key, dt.now(), update_val)

def update_all_keys(path: str, user: str):
	""" Updates all keys for given users """
	user_path = os.path.join(path, user)
	keys = os.listdir(user_path)
	return [update_batch(path, user, key) for key in keys]

def update_all_users(path: str):
	""" Updates all keys for given users """
	users = os.listdir(path)
	update_summaries = []
	for user in users:
		update_summaries.append(update_all_keys(path, user))
	return update_summaries
	