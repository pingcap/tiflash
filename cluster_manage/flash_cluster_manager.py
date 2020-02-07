#!/usr/bin/python3
import logging
import os
import socket
import time
from logging.handlers import RotatingFileHandler

import conf
import etcd
import flash_http_client
import placement_rule
import tidb_tools
import util
from pd_client import PDClient
import define

terminal: bool = False


def handle_receive_signal(signal_number, _):
    print('Received signal: ', signal_number)
    global terminal
    terminal = handle_receive_signal


def get_host():
    return socket.gethostbyname(socket.gethostname())


class TiFlashClusterNotMaster(Exception):
    def __init__(self):
        pass


def wrap_try_get_lock(func):
    def wrap_func(manager, *args, **kwargs):
        manager.try_get_lock()
        role, ts = manager.state
        if role == TiFlashClusterManager.ROLE_MASTER:
            return func(manager, *args, **kwargs)
        else:
            pass

    return wrap_func


class Store:
    TIFLASH_HTTP_PORT_LABEL = 'tiflash_http_port'

    def __eq__(self, other):
        return self.inner == other

    def __str__(self):
        return str(self.inner)

    def __init__(self, pd_store):
        self.inner = pd_store
        address = self.inner['address']
        host, port = address.split(':')
        self.ori_address = address
        self.ip = socket.gethostbyname(host)
        self.address = '{}:{}'.format(self.ip, port)
        self.tiflash_http_port = None
        self.tiflash_http_address = None
        for label in self.inner['labels']:
            if label['key'] == Store.TIFLASH_HTTP_PORT_LABEL:
                self.tiflash_http_port = int(label['value'])
                self.tiflash_http_address = '{}:{}'.format(self.ip, self.tiflash_http_port)

    @property
    def id(self):
        return self.inner['id']


class Table:
    def __init__(self, total_region, flash_region):
        self.total_region = total_region
        self.flash_region = flash_region


class TiFlashClusterManager:
    ROLE_INIT = 0
    ROLE_SLAVE = 1
    ROLE_MASTER = 2

    @staticmethod
    def compute_cur_store(stores):
        for _, store in stores.items():
            ok = store.ip == conf.flash_conf.service_ip
            # ok = True
            if ok and store.tiflash_http_port == conf.flash_conf.http_port:
                return store

        raise Exception("Can not tell current store.\nservice_addr: {},\nall tiflash stores: {}".format(
            conf.flash_conf.service_addr, [store.inner for store in stores.values()]))

    def _try_refresh(self):
        try:
            ori_role = self.state[0]
            self.pd_client.etcd_client.refresh_ttl(self.cur_store.address)
            self.state = [TiFlashClusterManager.ROLE_MASTER, time.time()]
            if ori_role == TiFlashClusterManager.ROLE_INIT:
                self.logger.debug('Continue become master')

        except etcd.EtcdValueError as e:
            self.state = [TiFlashClusterManager.ROLE_SLAVE, time.time()]
            self.logger.info('Refresh ttl fail become slave, %s', e.payload['message'])

        except etcd.EtcdKeyNotFound as e:
            self.state = [TiFlashClusterManager.ROLE_INIT, 0]
            self.try_get_lock()

    def try_get_lock(self):
        role, ts = self.state
        if role == TiFlashClusterManager.ROLE_INIT:
            if self.pd_client.etcd_client.try_init_mutex(self.cur_store.address):
                self.state = [TiFlashClusterManager.ROLE_MASTER, time.time()]
                self.logger.info('After init, become master')
            else:
                self.state = [TiFlashClusterManager.ROLE_SLAVE, time.time()]
                self.logger.info('After init, become slave')
        elif role == TiFlashClusterManager.ROLE_SLAVE:
            cur = time.time()
            if cur >= ts + conf.flash_conf.cluster_master_ttl:
                self.state = [TiFlashClusterManager.ROLE_INIT, 0]
                self.logger.info('Timeout, become init')
                self.try_get_lock()
        else:
            cur = time.time()
            if cur >= ts + conf.flash_conf.cluster_refresh_interval:
                self._try_refresh()

    def last_metrics_push_time(self):
        last_time = self.pd_client.etcd_client.get(define.METRICS_UPDATE_TIME)
        if last_time is None:
            last_time = 0
        else:
            last_time = float(last_time)
        return last_time

    def update_metrics_push_time(self):
        self.pd_client.etcd_client.write(define.METRICS_UPDATE_TIME, str(time.time()))

    def __init__(self, pd_client: PDClient, tidb_status_addr_list):
        self.logger = logging.getLogger('TiFlashManager')
        self.tidb_status_addr_list = tidb_status_addr_list
        self.pd_client = pd_client
        self.stores = {}
        self.cur_store = None
        self._update_cluster()

        self.state = [TiFlashClusterManager.ROLE_INIT, 0]
        self._try_refresh()
        self.table_update()

    def _update_cluster(self):
        prev_stores = self.stores
        self.stores = {store_id: Store(store) for store_id, store in
                       self.pd_client.get_store_by_labels(define.TIFLASH_LABEL).items()}
        if self.stores != prev_stores and prev_stores:
            self.logger.info('Update all tiflash stores: from {} to {}'.format([k.inner for k in prev_stores.values()],
                                                                               [k.inner for k in self.stores.values()]))
        self.cur_store = self.compute_cur_store(self.stores)

    def deal_with_region(self, region):
        for peer in region.peers:
            if peer.store_id == self.cur_store.id:
                assert peer.is_learner

    def _check_and_make_rule(self, table, start_key, end_key, all_rules: dict):
        rule_id = 'table-{}-r'.format(table['id'])

        need_new_rule = True
        if rule_id in all_rules:
            rule = all_rules[rule_id]
            if rule.override and rule.start_key == start_key and rule.end_key == end_key and rule.label_constraints == placement_rule.DEFAULT_LABEL_CONSTRAINTS and rule.location_labels == \
                table[define.LOCATION_LABELS] and rule.count == table[
                define.REPLICA_COUNT] and rule.role == define.LEARNER:
                need_new_rule = False

        if need_new_rule:
            rules_new = placement_rule.make_rule(rule_id, start_key, end_key, table[define.REPLICA_COUNT],
                                                 table[define.LOCATION_LABELS])
            self.set_rule(util.obj_2_dict(rules_new))

        all_rules.pop(rule_id, None)
        return need_new_rule

    @wrap_try_get_lock
    def set_rule(self, rule):
        if self.pd_client.set_rule(rule) == 200:
            self.logger.info('Set placement rule {}'.format(rule))
        else:
            raise Exception('Set placement rule {} fail'.format(rule))

    def compute_sync_data_process(self, table_id, start_key, end_key):
        stats_region: dict = self.pd_client.get_stats_region_by_range_json(start_key, end_key)
        region_count = stats_region.get('count', 0)
        flash_region_count = flash_http_client.get_region_count_by_table(self.stores.values(), table_id)
        return region_count, flash_region_count

    @wrap_try_get_lock
    def report_to_tidb(self, table, region_count, flash_region_count):
        self.logger.info(
            'report_to_tidb {} region_count: {} flash_region_count: {}'.format(table, region_count, flash_region_count))

        for idx, address in enumerate(self.tidb_status_addr_list):
            try:
                r = util.post_http(
                    '{}/tiflash/replica'.format(address, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION),
                    {"id": table['id'], define.REGION_COUNT: region_count,
                     define.TIFLASH_REGION_COUNT: flash_region_count})
                if r.status_code == 200:
                    if idx != 0:
                        tmp = self.tidb_status_addr_list[0]
                        self.tidb_status_addr_list[0] = address
                        self.tidb_status_addr_list[idx] = tmp
                    return
            except Exception:
                continue

        self.logger.error(
            'all tidb status addr {} can not be used'.format(self.tidb_status_addr_list))

    @wrap_try_get_lock
    def remove_rule(self, rule_id):
        self.pd_client.remove_rule(placement_rule.TIFLASH_GROUP_ID, rule_id)
        self.logger.info('Remove placement rule {}'.format(rule_id))

    @wrap_try_get_lock
    def try_push_metrics(self, metrics, table_cnt):
        last_time = self.last_metrics_push_time()
        if time.time() < last_time + conf.flash_conf.metrics_interval:
            return
        if conf.flash_conf.metrics_addr is not None:
            from prometheus_client import push_to_gateway, Info, CollectorRegistry, Gauge
            registry = CollectorRegistry()

            g = Gauge(define.REPLICA_SYNC, define.REPLICA_SYNC_DISC, [define.TABLE_ID], registry=registry)
            for tid, cnt in metrics.items():
                g.labels(str(tid)).set('%.2f' % (1.0 * cnt[1] / cnt[0] if cnt[0] > 0 else 0.0))

            leader = Info(define.STATISTIC_INFO, define.STATISTIC_INFO_DISC, registry=registry)
            leader.info({define.LEADER_ADDR: self.cur_store.ori_address, define.TOTAL_TABLE_CNT: str(table_cnt),
                         define.UNAVAILABLE_TABLE_CNT: str(len(metrics))})

            push_to_gateway(conf.flash_conf.metrics_addr, job=define.TIFLASH_REPLICA_STATUS, registry=registry,
                            timeout=conf.flash_conf.update_rule_interval)
        self.update_metrics_push_time()

    @wrap_try_get_lock
    def table_update(self):
        table_list = tidb_tools.db_flash_replica(self.tidb_status_addr_list)
        all_rules = self.pd_client.get_group_rules(placement_rule.TIFLASH_GROUP_ID)
        metrics = {}
        for table in table_list:
            from tikv_util import common

            table_id = table['id']
            st, ed = common.make_table_begin(table_id), common.make_table_end(table_id)
            start_key, end_key = st.to_bytes(), ed.to_bytes()
            self._check_and_make_rule(table, st.to_pd_key(), ed.to_pd_key(), all_rules)

            if not table[define.AVAILABLE]:
                region_count, flash_region_count = self.compute_sync_data_process(table_id, start_key, end_key)
                self.report_to_tidb(table, region_count, flash_region_count)
                metrics[table_id] = (region_count, flash_region_count)

        for rule in all_rules.values():
            self.remove_rule(rule.id)

        self.try_push_metrics(metrics, len(table_list))


def main():
    flash_conf = conf.flash_conf
    parent_path = os.path.dirname(flash_conf.log_path)
    if not os.path.exists(parent_path):
        os.makedirs(parent_path)

    # keep at most 10G log files
    logging.basicConfig(
        handlers=[RotatingFileHandler(flash_conf.log_path, maxBytes=1024 * 1024 * 1024, backupCount=10)],
        level=conf.log_level, format='%(asctime)s <%(levelname)s> %(name)s: %(message)s')
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    logging.debug('\nCluster Manager Version Info\n{}'.format(conf.version_info))

    try:
        pd_client = PDClient(flash_conf.pd_addrs)
        TiFlashClusterManager(pd_client, conf.flash_conf.tidb_status_addr)
    except Exception as e:
        logging.exception(e)


if __name__ == '__main__':
    main()
