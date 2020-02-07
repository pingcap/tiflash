#!/usr/bin/python3
import logging
from typing import Optional

import define
import etcd
import uri
import conf
import util


class EtcdClient:
    def try_init_mutex(self, cluster_mutex_value):
        try:
            res = self.client.write(define.TIFLASH_CLUSTER_MUTEX_KEY,
                                    cluster_mutex_value,
                                    ttl=conf.flash_conf.cluster_master_ttl, prevExist=False)
            self.logger.info('Try to init master success, ttl: %d, create new key: %s', res.ttl, res.key)
            return True
        except etcd.EtcdAlreadyExist as e:
            self.logger.info('Try to init master fail, %s', e.payload['message'])
        return False

    def refresh_ttl(self, cluster_mutex_value):
        self.client.refresh(define.TIFLASH_CLUSTER_MUTEX_KEY, conf.flash_conf.cluster_master_ttl,
                            prevValue=cluster_mutex_value)

    def get(self, key):
        try:
            return self.client.get(key).value
        except etcd.EtcdKeyError as _:
            return None

    def write(self, key, value):
        self.client.write(key, value, ttl=conf.flash_conf.cluster_master_ttl)

    def __init__(self, host, port):
        self.logger = logging.getLogger('etcd.client')
        self.client = etcd.Client(host=host, port=port)


class PDClient:
    PD_API_PREFIX = 'pd/api'
    PD_API_VERSION = 'v1'

    def get_all_regions_json(self):
        r = util.curl_http('{}/{}/{}/regions'.format(self.leader, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION))
        return r.json()

    def get_regions_by_key_json(self, key: str, limit=16):
        r = util.curl_http(
            '{}/{}/{}/regions/key'.format(self.leader, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION),
            {'key': key, 'limit': limit})
        return r.json()

    def get_region_by_id_json(self, region_id: int):
        r = util.curl_http(
            '{}/{}/{}/region/id/{}'.format(self.leader, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION, region_id))
        return r.json()

    def get_all_stores_json(self):
        r = util.curl_http(
            '{}/{}/{}/stores'.format(self.leader, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION))
        return r.json()

    def get_members_json(self, *args):
        url = args[0] if args else self.leader
        r = util.curl_http(
            '{}/{}/{}/members'.format(url, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION))
        return r.json()

    def get_stats_region_by_range_json(self, start_key, end_key):
        r = util.curl_http(
            '{}/{}/{}/stats/region'.format(self.leader, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION),
            {'start_key': start_key, 'end_key': end_key},
        )
        return r.json()

    def get_group_rules(self, group):
        r = util.curl_http(
            '{}/{}/{}/config/rules/group/{}'.format(self.leader, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION,
                                                    group))
        res = r.json()
        res = res if res is not None else {}
        for e in res:
            if not isinstance(e, dict):
                raise Exception('Got placement rules fail: {}'.format(r.text))
        from placement_rule import PlacementRule
        return {e['id']: PlacementRule(**e) for e in res}

    def get_all_rules(self):
        r = util.curl_http(
            '{}/{}/{}/config/rules'.format(self.leader, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION))
        res = r.json()
        return res if res is not None else {}

    def set_rule(self, rule):
        r = util.post_http(
            '{}/{}/{}/config/rule'.format(self.leader, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION), rule)
        return r.status_code

    def remove_rule(self, group, rule_id):
        r = util.delete_http(
            '{}/{}/{}/config/rule/{}/{}'.format(self.leader, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION, group,
                                                rule_id))
        return r.status_code

    def _update_leader_etcd(self):
        for url in self.urls:
            resp = self.get_members_json(url)
            leader = resp.get('leader', {})
            client_urls = leader.get('client_urls', [])
            if client_urls:
                _client_urls = []
                for member in resp.get('members', {}):
                    _client_urls.extend(member.get('client_urls', []))
                self.urls = _client_urls
                self.leader = uri.URI(client_urls[0]).authority
                _etcd_leader_uri = uri.URI(resp.get('etcd_leader', {}).get('client_urls', [])[0])
                self.etcd_client = EtcdClient(_etcd_leader_uri.host, _etcd_leader_uri.port)
                return
        raise Exception("can not find pd leader")

    def get_store_by_labels(self, flash_label):
        res = {}
        all_stores = self.get_all_stores_json()
        for store in all_stores['stores']:
            store = store['store']
            for label in store.get('labels', []):
                if label == flash_label:
                    res[store['id']] = store
        return res

    def __init__(self, urls):
        self.logger = logging.getLogger('pd.client')

        self.urls = urls
        self.leader = ""
        self.etcd_client: Optional[EtcdClient] = None
        self._update_leader_etcd()


def main():
    pass


if __name__ == '__main__':
    main()
