import time
import snscrape.modules.reddit as snreddit
import pickle
import time
import datetime
import pandas as pd
import json


# completed
####### ADA ###########
# 2017-11-09 = 1483920000
# start_date = time.mktime(datetime.datetime.strptime('09/01/2017', "%d/%m/%Y").timetuple())
# end_date = time.mktime(datetime.datetime.strptime('05/07/2022', "%d/%m/%Y").timetuple())
# print(start_date)
# print(end_date)
# snscrape --jsonl reddit-search  --before 1656975600 --after 1483920000 'cardano' > ada-reddit.json
####### ADA ###########

####### AVAX ###########
# avax - 2020-07-13
# start_date = time.mktime(datetime.datetime.strptime('13/07/2020', "%d/%m/%Y").timetuple())
# end_date = time.mktime(datetime.datetime.strptime('05/07/2022', "%d/%m/%Y").timetuple())
# print(start_date)
# print(end_date)
# snscrape --jsonl reddit-search  --before 1656975600 --after 1594594800.0 'cardano' > ada-reddit.json
####### AVAX ###########


####### DOT ###########
# avax - 2020-08-20
# start_date = time.mktime(datetime.datetime.strptime('20/08/2020', "%d/%m/%Y").timetuple())
# end_date = time.mktime(datetime.datetime.strptime('05/07/2022', "%d/%m/%Y").timetuple())
# print(start_date)
# print(end_date)
# snscrape --jsonl reddit-search  --before 1656975600 --after 1483920000 'cardano' > ada-reddit.json
####### DOT ###########

####### SOL ###########
# 2020-04-10 = 1483920000
# start_date = time.mktime(datetime.datetime.strptime('10/04/2020', "%d/%m/%Y").timetuple())
# end_date = time.mktime(datetime.datetime.strptime('05/07/2022', "%d/%m/%Y").timetuple())
# print(start_date)
# print(end_date)
# snscrape --jsonl reddit-search  --before 1656975600 --after 1586473200 'solana' > sol-reddit.json
####### SOL ###########


####### MATIC ###########
# 2019-04-28 = 1483920000
# start_date = time.mktime(datetime.datetime.strptime('28/04/2019', "%d/%m/%Y").timetuple())
# end_date = time.mktime(datetime.datetime.strptime('05/07/2022', "%d/%m/%Y").timetuple())
# print(start_date)
# print(end_date)
# snscrape --jsonl reddit-search  --before 1656975600 --after 1586473200 'matic' > matic-reddit.json
####### SOL ###########

