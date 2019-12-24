#!/usr/bin/python3

# https://github.com/pingcap/tidb/blob/master/docs/tidb_http_api.md

import util


def curl_tidb(address, uri):
    r = util.curl_http('{}{}'.format(address, uri))
    if r.status_code == 200:
        return r.json()
    else:
        return None


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
    for idx, address in enumerate(tidb_status_addr_list):
        try:
            r = curl_tidb(address, '/tiflash/replica')
            if r is not None:
                if idx != 0:
                    tmp = tidb_status_addr_list[0]
                    tidb_status_addr_list[0] = address
                    tidb_status_addr_list[idx] = tmp
                return r
        except Exception:
            continue

    raise Exception('all tidb status addr {} can not be used'.format(tidb_status_addr_list))


def main():
    pass


if __name__ == '__main__':
    main()
