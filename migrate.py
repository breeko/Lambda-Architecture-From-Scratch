from utils.db.utils import *
import os
import csv
import re
from datetime import datetime as dt
from dateutil import parser
from math import sqrt

BASE_DIR = "/home/branko/reddit/"
DB_PATH = "/home/branko/reddit/logs/botrank"
LOG_PATH = "/home/branko/reddit/logs/B0tRank.log"

### MIGRATE

def wilson_score(ups, downs, z=1.96):
    n = ups + downs
    if n == 0:
        return 0
    phat = float(ups) / n
    return ((phat + z*z/(2*n) - z * sqrt((phat*(1-phat)+z*z/(4*n))/n))/(1+z*z/n))

def migrate_csv( csv_path: str, path: str, user_field: str, value_field: str, key: str, case_sensitive=False):
	with open(csv_path, 'r') as f:
		reader = csv.reader(f)
		headers = next(reader)
		user_field_idx = headers.index(user_field)
		value_field_idx = headers.index(value_field)
		for row in reader:
			user = row[user_field_idx]
			val = row[value_field_idx]
			if not case_sensitive:
				user = user.lower()
			add_record(path, user, key, dt.min, val)
	return path

def migrate_logs(log_path: str, filter_func, user_func, key_func, val_func, time_func, path: str, case_sensitive=False):
	with open(log_path, "r") as f:
		for line in f:
			if filter_func(line):
				val = val_func(line)
				key = key_func(line)
				user = user_func(line)
				time = time_func(line)
				if val and key and user and time:
					if not case_sensitive:
						key = key.lower()
						user = user.lower()	
					add_record(path, user, key, time, val)
	return path

def write_to_csv(path: str, log_path:str, csv_path: str):
	out = {}
	user_proper = get_user_proper(log_path)
	users = os.listdir(path)
	for user in users:
		user_dir = os.path.join(path, user)
		keys = os.listdir(user_dir)
		for key in keys:
			val = get_latest_batch(path, user, key, val=True)
			if user not in out:
				out[user] = {}
			out[user][key] = val
	for bot, d in out.items():
		good = d.get("good",0)
		bad = d.get("bad",0)
		score = round(wilson_score(good, bad), 4)
		out[bot]["score"] = score
	for idx, key in enumerate(sorted(out, key=lambda k: out[k].get("score"), reverse=True)):
		out[key]["rank"] = idx + 1
		out[key]["name"] = user_proper.get(key, key)
	return save_csv(out, csv_path)

def get_logs_grouped(log_path: str, group_func, val_func, keep_last=True):
	grouped = {}
	with open(log_path, "r") as f:
		for line in f:
			group = group_func(line)
			val = val_func(line)
			if group and val:
				if group not in grouped or keep_last:
					grouped[group] = val
	return grouped

def get_user_proper(log_path):
	user_func = lambda l: get_regex_group(r"(?<=action: )([_\-0-9a-zA-Z]+)", l, re.IGNORECASE)
	user_lower_func = lambda l: user_func(l).lower() if user_func(l) else None
	return get_logs_grouped(log_path, user_lower_func, user_func, keep_last=True)

def save_csv(d, filename):
    keys = ["key", "rank", "name", "score", "good-votes", "bad-votes" ]
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction="ignore")
        dict_writer.writeheader()
        lines = [{	"key": k,
					"rank": v["rank"], 
					"name": v["name"], 
					"score": v["score"], 
					"good-votes": v.get("good",0), 
					"bad-votes": v.get("bad",0)} for k, v in d.items()]
        dict_writer.writerows(lines)
    return filename

def flatten(l):
    return [item for sublist in l for item in sublist]

def update_and_write(db_path: str, log_path: str, csv_path: str):
	_ = update_all_users(db_path)
	write_to_csv(db_path, log_path, csv_path)

# Migrating

# migrate_csv("logs/bots-original.csv", DB_PATH, "name", "good-votes", "good")
# migrate_csv("logs/bots-original.csv", DB_PATH, "name", "bad-votes", "bad")

# filter_func = lambda l: "INFO" in l
# key_func = lambda l: get_regex_group(r"(?<=comment: )([a-zA-Z]+)", l, re.IGNORECASE)
# user_func = lambda l: get_regex_group(r"(?<=action: )([_\-0-9a-zA-Z]+)", l, re.IGNORECASE)
# val_func = lambda l: 1
# time_func = lambda l: parser.parse(l.split(" - ")[0])

# _ = migrate_logs(LOG_PATH, filter_func, user_func, key_func, val_func, time_func, DB_PATH)
# _ = batch_all_users(DB_PATH)


