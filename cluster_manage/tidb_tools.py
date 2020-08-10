#!/usr/bin/python3

# https://github.com/pingcap/tidb/blob/master/docs/tidb_http_api.md

import util


def curl_tidb(address, uri):
    r = util.curl_http('{}{}'.format(address, uri))
    return util.try_get_json(r)


def status(address):
    return curl_tidb(address, '/status')


def table_by_id(address, table_id):
    return curl_tidb(address, '/schema?table_id={}'.format(table_id))


def db_info(address, table_id):
    return curl_tidb(address, '/db-table/{}'.format(table_id))


def db_schema(address, db):
    return curl_tidb(address, '/schema/{}'.format(db))


def db_all_schema(address):
    return curl_tidb(address, '/schema')


def db_flash_replica(tidb_status_addr_list):
    error_list = []
    for idx, address in enumerate(tidb_status_addr_list):
        try:
            return curl_tidb(address, '/tiflash/replica')
        except Exception as e:
            error_list.append((address, e))
            continue

    raise Exception('can not get tiflash replica info from tidb: {}'.format(error_list))


def main():
    pass


if __name__ == '__main__':
    main()
