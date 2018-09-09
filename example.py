from utils import *
from datetime import datetime as dt

BASE_DIR = "/home/branko/reddit/logs/botrank"
USER = "___alexa___"
USER2 = "AddedColor"

# Adding a base record
add_record(BASE_DIR, USER, "good", dt.min, val=100)
# '.../botrank/___alexa___/good/1/01/01/1-01-01 00:00:00/100'

# Retrieve last summary
get_latest_batch(BASE_DIR, USER, "good", val=True) 
# None (nothing batched yet)

# Batching all records
batch(BASE_DIR, USER, "good")
# '/home/branko/reddit/logs/botrank/___alexa___/good/summary/2018/09/08/2018-09-08 14:15:52/100'

# Retrieving latest batch value
get_latest_batch_time(BASE_DIR, USER, "good")
# datetime.datetime(2018, 9, 8, 14, 15, 52)

# Retrieve last summary
get_latest_batch(BASE_DIR, USER, "good", val=True)
# 100

# Adding a record
add_record(BASE_DIR, USER, "good", dt.now())
# '/home/branko/reddit/logs/botrank/___alexa___/good/2018/09/08/2018-09-08 14:16:43/1'

# Retrieving latest batch
get_latest_batch(BASE_DIR, USER, "good", val=True)
# '100' (record we just added hasn't been batched yet)

# Batching all records for a given key
batch(BASE_DIR, USER, "good")
# '/home/branko/reddit/logs/botrank/___alexa___/good/summary/2018/09/08/2018-09-08 14:17:04/101'

# Retrieving latest batch
get_latest_batch(BASE_DIR, USER, "good", val=True)
# '101' (record we just added has now been batched)

# Adding a record with multiple values
add_record(BASE_DIR, USER, "good", dt.now(), val=3)
# '/home/branko/reddit/logs/botrank/___alexa___/good/2018/09/08/2018-09-08 14:17:20/3'

# Updating batch based on prior summary and new records added
update_batch(BASE_DIR, USER, "good")
# '/home/branko/reddit/logs/botrank/___alexa___/good/summary/2018/09/08/2018-09-08 14:17:33/104'

# Adding a log for another key
add_record(BASE_DIR, USER, "bad", dt.now())
# '/home/branko/reddit/logs/botrank/___alexa___/bad/2018/09/08/2018-09-08 14:17:48/1'

# Batching all keys
batch_all_keys(BASE_DIR, USER)
# ['/home/branko/reddit/logs/botrank/___alexa___/bad/summary/2018/09/08/2018-09-08 14:18:28/1', 
# '/home/branko/reddit/logs/botrank/___alexa___/good/summary/2018/09/08/2018-09-08 14:18:28/104']

# Adding a new user
add_record(BASE_DIR, USER2, "good", dt.now())
# '/home/branko/reddit/logs/botrank/AddedColor/good/2018/09/08/2018-09-08 14:18:41/1'

# Batching all users
batch_all_users(BASE_DIR)
# [['/home/branko/reddit/logs/botrank/___alexa___/bad/summary/2018/09/08/2018-09-08 14:18:52/1', 
# 	'/home/branko/reddit/logs/botrank/___alexa___/good/summary/2018/09/08/2018-09-08 14:18:52/104'], 
#  ['/home/branko/reddit/logs/botrank/AddedColor/good/summary/2018/09/08/2018-09-08 14:18:52/1']]
