#!/usr/bin/python3
import argparse
import logging

import define
import flash_http_client
from pd_client import PDClient


class Store:
    def __eq__(self, other):
        return self.inner == other

    def __str__(self):
        return str(self.inner)

    def __init__(self, pd_store):
        self.inner = pd_store
        address = self.inner['address']
        host, port = address.split(':')
        self.address = '{}:{}'.format(host, port)
        _, status_port = self.inner['status_address'].split(':')
        self.tiflash_status_address = '{}:{}'.format(host, status_port)

    @property
    def id(self):
        return self.inner['id']


class Runner:
    def __init__(self):
        parser = argparse.ArgumentParser(description="check compress statistic",
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument(
            '--pd-address', default="172.16.4.39:2174")
        parser.add_argument(
            '--type', help='run type', required=True, choices=('show', 'clean', 'set', ))
        parser.add_argument(
            '--method', choices=('SNAPPY', 'LZ4', 'ZSTD', 'NONE'))
        self.args = parser.parse_args()
        try:
            assert self.args.pd_address
            pd_address = self.args.pd_address.split(",")
            pd_client = PDClient(pd_address)
            self.tiflash_stores = {store_id: Store(store) for store_id, store in pd_client.get_store_by_labels(
                define.TIFLASH_LABEL).items()}
        except Exception as e:
            logging.exception(e)

    def run(self):
        if self.args.type == 'show':
            self.run_show()
        elif self.args.type == 'clean':
            self.run_clean()
        elif self.args.type == 'set':
            self.run_set_method()
        elif self.args.type == 'get-cnt':
            self.run_show_cnt()
        elif self.args.type == 'get-encode':
            self.run_get_encode()

    def run_clean(self):
        res = {store_id: flash_http_client.clean_compress_info(
            store.tiflash_status_address) for store_id, store in self.tiflash_stores.items()}
        print(res)

    def run_set_method(self):
        assert self.args.method
        res = {store_id: flash_http_client.set_compress_method(
            store.tiflash_status_address, self.args.method) for store_id, store in self.tiflash_stores.items()}
        print(res)

    def run_show(self):
        compress_info = {store_id: flash_http_client.get_compress_info(
            store.tiflash_status_address) for store_id, store in self.tiflash_stores.items()}
        tol_compressed_size = 0
        tol_uncompressed_size = 0
        method = None
        for store_id, info in compress_info.items():
            print('store {}, addr {}, {}'.format(
                store_id,  self.tiflash_stores[store_id].tiflash_status_address, info))
            tol_compressed_size += int(info['compressed_size'])
            tol_uncompressed_size += int(info['uncompressed_size'])
            if method:
                assert method == info['method']
            else:
                method = info['method']
        compress_rate = 0.0 if not tol_uncompressed_size else tol_compressed_size / \
            tol_uncompressed_size
        MB = 1024**2
        msg = 'method: {}, compress_rate: {:.3f}, compressed_size: {:.3f}MB, uncompressed_size: {:.3f}MB'.format(
            method, compress_rate,
            tol_compressed_size/MB, tol_uncompressed_size/MB,
        )
        print(msg)

    def run_show_cnt(self):
        compress_info = {store_id: flash_http_client.get_stream_info(
            store.tiflash_status_address) for store_id, store in self.tiflash_stores.items()}
        for store_id, info in compress_info.items():
            print('store {}: {}'.format(store_id, info))

    def run_get_encode(self):
        compress_info = {store_id: flash_http_client.get_codec_info(
            store.tiflash_status_address) for store_id, store in self.tiflash_stores.items()}
        all_bytes = {}
        all_time = {}
        all_hash_row = {}
        all_hash_time = {}
        for store_id, info in compress_info.items():
            all_bytes[store_id] = 0
            all_time[store_id] = 0
            all_hash_row[store_id] = 0
            all_hash_time[store_id] = 0
            x = [x for x in info.split(",")]
            for o in x:
                a, b = o.split(':')
                a, b = a.strip(), int(b.strip())
                if a == 'uncompress-bytes':
                    all_bytes[store_id] += b
                elif a == 'time':
                    all_time[store_id] += b
                elif a == 'hash-part-write':
                    all_hash_row[store_id] += b
                elif a == 'hash-part-time':
                    all_hash_time[store_id] += b

            all_time[store_id] = all_time[store_id]/(10**9)
            all_hash_time[store_id] = all_hash_time[store_id]/(10**9)
            all_bytes[store_id] /= 1024 ** 2
            y = all_bytes[store_id] / \
                all_time[store_id] if all_time[store_id] else 0.0
            print("store {}: uncompress-bytes: {:.4f}MB, time: {:.4f}s, MBPS/core: {:.4f}, hash-part-write: {}rows, hash-part-time: {:.4f}s".format(
                store_id, all_bytes[store_id], all_time[store_id], y, all_hash_row[store_id], all_hash_time[store_id]))


if __name__ == '__main__':
    Runner().run()
