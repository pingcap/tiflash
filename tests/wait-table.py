#!/usr/bin/env python2

import os
import time

from sys import argv


if len(argv) < 4:
    print('Usage: {} [database] [tables...] [MySQL client]'.format(argv[0]))
    exit(1)

database = argv[1]
tables = argv[2:-1]
client = argv[-1]

timeout = 600
sleep_time = 1.0

table_full_names = ', '.join('{}.{}'.format(database, table) for table in tables)
print('=> wait for {} available in TiFlash'.format(table_full_names))

table_names = ', '.join("'{}'".format(table) for table in tables)
query = "select sum(available) from information_schema.tiflash_replica where table_schema='{}' and table_name in ({})".format(database, table_names)

start_time = time.time()

available = False
retry_count = 0
while True:
    for line in os.popen('{} "{}"'.format(client, query)).readlines():
        try:
            count = int(line.strip())
            if count == len(tables):
                available = True
                break
        except:
            pass

    if available:
        break

    retry_count += 1
    if retry_count % 10 == 0:
        print('=> waiting for {} available'.format(table_full_names))

    time_used = time.time() - start_time
    if time_used >= timeout:
        break
    else:
        # if it is near to timeout, sleep time will be shorter and then give it the last try.
        time.sleep(min(sleep_time, timeout - time_used))

time_used = time.time() - start_time

if available:
    print('=> all tables are available now. time = {}s'.format(time_used))
else:
    print('=> cannot sync tables in {}s'.format(time_used))
    exit(1)
