#!/usr/bin/env python3

import os
import time

from sys import argv


if len(argv) < 4:
    print(f'Usage: {argv[0]} [database] [tables...] [MySQL client]')
    exit(1)

database = argv[1]
tables = argv[2:-1]
client = argv[-1]

timeout = 600
sleep_time = 1.0

table_full_names = ', '.join(f'{database}.{table}' for table in tables)
print(f'=> wait for {table_full_names} available in TiFlash')

table_names = ', '.join(f"'{table}'" for table in tables)
query = f"select sum(available) from information_schema.tiflash_replica where table_schema='{database}' and table_name in ({table_names})"

start_time = time.time()

available = False
for timestamp in range(timeout):
    for line in os.popen(f'{client} "{query}"').readlines():
        try:
            count = int(line.strip())
            if count == len(tables):
                available = True
                break
        except:
            pass

    if available:
        break
    if timestamp % 10 == 0:
        print(f'=> waiting for {table_full_names} available')

    time.sleep(sleep_time)

end_time = time.time()
time_used = end_time - start_time

if available:
    print(f'=> all tables are available now. time = {time_used}s')
else:
    print(f"=> cannot sync tables in {time_used}s")
    exit(1)
