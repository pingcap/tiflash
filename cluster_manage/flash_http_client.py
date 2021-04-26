#!/usr/bin/python3

import util


def curl_flash(address, params):
    if type(params) != dict:
        params = {'query': params}
    r = util.curl_http(address, params)
    return r


def get_region_count_by_table(store_list, table_id, replica_count):
    from tikv_util import common

    checker = common.CheckRegionCnt()
    err = []
    for store in store_list:
        try:
            res = util.curl_http('{}/tiflash/sync-status/{}'.format(store.tiflash_status_address, table_id))
            util.check_status_code(res)
            checker.add(res.content)
        except Exception as e:
            err.append(e)
    return checker.compute(replica_count), err


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
