#!/usr/bin/python3
import requests

import util


def curl_flash(address, params):
    if type(params) != dict:
        params = {'query': params}
    r = util.curl_http(address, params)
    return r


def get_region_count_by_table(store_list, table_id):
    from tikv_util import common

    checker = common.CheckRegionCnt()
    for store in store_list:
        sql = 'DBGInvoke dump_region_table({},true)'.format(table_id)
        try:
            res = curl_flash(store.tiflash_http_address, sql)
            checker.add(res.content)
        except requests.exceptions.RequestException:
            continue
    return checker.compute()


def get_regions_by_range(address, start_key, end_key):
    sql = "DBGInvoke find_region_by_range(\'{}\',\'{}\', 1)".format(start_key, end_key)
    res = curl_flash(address, sql).text
    res = res.split('\n')[:-1]
    res[1] = res[1].split(' ')[1:-1]
    return res


def get_region_count_by_range(address, start_key, end_key):
    sql = "DBGInvoke find_region_by_range(\'{}\',\'{}\')".format(start_key, end_key)
    res = curl_flash(address, sql).text.split('\n')
    return int(res[0])


def main():
    pass


if __name__ == '__main__':
    main()
