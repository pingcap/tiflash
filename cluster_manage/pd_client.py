#!/usr/bin/python3
import logging
from typing import Optional

import define
import etcd3
import uri
import conf
import util


class EtcdClient:
    EtcdOK = 0
    EtcdKeyNotFound = 1
    EtcdValueNotEqual = 2

    def try_init_mutex(self, cluster_mutex_value):
        val, meta = self.client.get(define.TIFLASH_CLUSTER_MUTEX_KEY)
        if val is None:
            lease = self.client.lease(conf.flash_conf.cluster_master_ttl)
            if self.client.put_if_not_exists(define.TIFLASH_CLUSTER_MUTEX_KEY, cluster_mutex_value, lease=lease):
                self.logger.info('Try to init master success, ttl: %d, create new key: %s', lease.ttl,
                                 define.TIFLASH_CLUSTER_MUTEX_KEY)
                return True
        self.logger.info('Try to init master fail, key exists')
        return False

    def refresh_ttl(self, cluster_mutex_value):
        val, meta = self.client.get(define.TIFLASH_CLUSTER_MUTEX_KEY)
        if val is None:
            return self.EtcdKeyNotFound
        if cluster_mutex_value != str(val, encoding="utf8"):
            return self.EtcdValueNotEqual
        list(self.client.refresh_lease(meta.lease_id))
        return self.EtcdOK

    def get_by_prefix(self, prefix):
        return self.client.get_prefix(prefix)

    def get(self, key):
        return self.client.get(key)

    def put(self, key, val):
        return self.client.put(key, val)

    def update(self, key, value, ttl=conf.flash_conf.cluster_master_ttl):
        ori_val, meta = self.client.get(key)
        if ori_val is None or value != str(ori_val, encoding="utf8"):
            self.client.put(key, value, self.client.lease(ttl))
        else:
            list(self.client.refresh_lease(meta.lease_id))

    def __init__(self, host, port):
        self.logger = logging.getLogger('etcd.client')
        kwargs = {"timeout": conf.flash_conf.max_time_out}
        if conf.flash_conf.enable_tls:
            kwargs["ca_cert"] = conf.flash_conf.ca_path
            kwargs["cert_key"] = conf.flash_conf.key_path
            kwargs["cert_cert"] = conf.flash_conf.cert_path
        self.client = etcd3.client(host=host, port=port, **kwargs)


class PDClient:
    PD_API_PREFIX = 'pd/api'
    PD_API_VERSION = 'v1'

    def get_all_regions_json(self):
        r = util.curl_http('{}/{}/{}/regions'.format(self.leader, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION))
        return util.try_get_json(r)

    def get_regions_by_key_json(self, key: str, limit=16):
        r = util.curl_http(
            '{}/{}/{}/regions/key'.format(self.leader, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION),
            {'key': key, 'limit': limit})
        return util.try_get_json(r)

    def get_region_by_id_json(self, region_id: int):
        r = util.curl_http(
            '{}/{}/{}/region/id/{}'.format(self.leader, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION, region_id))
        return util.try_get_json(r)

    def get_all_stores_json(self):
        r = util.curl_http(
            '{}/{}/{}/stores'.format(self.leader, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION))
        return util.try_get_json(r)

    def get_members_json(self, *args):
        url = args[0] if args else self.leader
        r = util.curl_http(
            '{}/{}/{}/members'.format(url, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION))
        return util.try_get_json(r)

    def get_stats_region_by_range_json(self, start_key, end_key):
        r = util.curl_http(
            '{}/{}/{}/stats/region'.format(self.leader, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION),
            {'start_key': start_key, 'end_key': end_key},
        )
        return util.try_get_json(r)

    def get_group_rules(self, group):
        r = util.curl_http(
            '{}/{}/{}/config/rules/group/{}'.format(self.leader, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION,
                                                    group))
        res = util.try_get_json(r)
        res = res if res is not None else {}
        for e in res:
            if not isinstance(e, dict):
                raise Exception('Got placement rules fail: {}'.format(r.text))
        from placement_rule import PlacementRule
        return {e['id']: PlacementRule(**e) for e in res}

    def get_all_rules(self):
        r = util.curl_http(
            '{}/{}/{}/config/rules'.format(self.leader, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION))
        res = util.try_get_json(r)
        return res if res is not None else {}

    def get_rule(self, group, rule_id):
        r = util.curl_http(
            '{}/{}/{}/config/rule/{}/{}'.format(self.leader, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION, group,
                                                rule_id))
        return util.try_get_json(r)

    def set_rule(self, rule):
        r = util.post_http(
            '{}/{}/{}/config/rule'.format(self.leader, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION), rule)
        return r.status_code

    def set_accelerate_schedule(self, start_key, end_key):
        r = util.post_http(
            '{}/{}/{}/regions/accelerate-schedule'.format(self.leader, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION),
            {'start_key': start_key, 'end_key': end_key}, )
        if r.status_code != 200:
            raise Exception(
                "fail to accelerate schedule range [{},{}), error msg: {}".format(start_key, end_key, r.text))

    def remove_rule(self, group, rule_id):
        r = util.delete_http(
            '{}/{}/{}/config/rule/{}/{}'.format(self.leader, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION, group,
                                                rule_id))
        return r.status_code

    def _try_update_leader_etcd(self, url):
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

    def _update_leader_etcd(self):
        errors = []
        for url in self.urls:
            try:
                return self._try_update_leader_etcd(url)
            except Exception as e:
                errors.append(e)
        raise Exception("can not find pd leader: {}".format(errors))

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
