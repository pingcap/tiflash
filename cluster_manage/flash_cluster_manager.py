#!/usr/bin/python3
import logging
import os
import socket
import time
from logging.handlers import RotatingFileHandler

import conf
import flash_http_client
import placement_rule
import tidb_tools
import util
from pd_client import PDClient, EtcdClient
import define

terminal: bool = False


def handle_receive_signal(signal_number, _):
    print('Received signal: ', signal_number)
    global terminal
    terminal = handle_receive_signal


def get_host():
    return socket.gethostbyname(socket.gethostname())


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
    def __eq__(self, other):
        return self.inner == other

    def __str__(self):
        return str(self.inner)

    def __init__(self, pd_store):
        self.inner = pd_store
        address = self.inner['address']
        host, port = address.split(':')
        self.ip = socket.gethostbyname(host)
        self.address = '{}:{}'.format(self.ip, port)
        self.tiflash_http_address = None

    @property
    def id(self):
        return self.inner['id']


class Table:
    def __init__(self, total_region, flash_region):
        self.total_region = total_region
        self.flash_region = flash_region


class TiFlashClusterManager:
    ROLE_INIT = 0
    ROLE_SECONDARY = 1
    ROLE_MASTER = 2

    @staticmethod
    def compute_cur_store(stores):
        for _, store in stores.items():
            if store.address == conf.flash_conf.service_addr:
                return store

        raise Exception("Can not tell current store.\nservice_addr: {},\nall tiflash stores: {}".format(
            conf.flash_conf.service_addr, [store.inner for store in stores.values()]))

    def _try_refresh(self):
        ori_role = self.state[0]
        res = self.pd_client.etcd_client.refresh_ttl(self.cur_store.address)
        if res == EtcdClient.EtcdOK:
            self.state = [TiFlashClusterManager.ROLE_MASTER, time.time()]
            if ori_role == TiFlashClusterManager.ROLE_INIT:
                self.logger.debug('Continue become master')
        elif res == EtcdClient.EtcdKeyNotFound:
            self.state = [TiFlashClusterManager.ROLE_INIT, 0]
            self.try_get_lock()
        elif res == EtcdClient.EtcdValueNotEqual:
            self.state = [TiFlashClusterManager.ROLE_SECONDARY, time.time()]
            self.logger.debug('Refresh ttl fail (key not equal), become secondary')
        else:
            assert False

    def try_get_lock(self):
        role, ts = self.state
        if role == TiFlashClusterManager.ROLE_INIT:
            if self.pd_client.etcd_client.try_init_mutex(self.cur_store.address):
                self.state = [TiFlashClusterManager.ROLE_MASTER, time.time()]
                self.logger.info('After init, become master')
            else:
                self.state = [TiFlashClusterManager.ROLE_SECONDARY, time.time()]
                self.logger.info('After init, become secondary')
        elif role == TiFlashClusterManager.ROLE_SECONDARY:
            cur = time.time()
            if cur >= ts + conf.flash_conf.cluster_master_ttl:
                self.state = [TiFlashClusterManager.ROLE_INIT, 0]
                self.logger.info('Timeout, become init')
                self.try_get_lock()
        else:
            cur = time.time()
            if cur >= ts + conf.flash_conf.cluster_refresh_interval:
                self._try_refresh()

    def _get_http_port_for_all_store(self):
        http_info_map = {}
        for value, metadata in self.pd_client.etcd_client.get_by_prefix(define.TIFLASH_CLUSTER_HTTP_PORT):
            key = str(metadata.key, encoding='utf-8')[len(define.TIFLASH_CLUSTER_HTTP_PORT):]
            http_info_map[key] = str(value, encoding='utf-8')

        for store in self.stores.values():
            store.tiflash_http_address = http_info_map.get(store.address)

        self.logger.debug('http port of all store: {}'.format(http_info_map))

    def _update_http_port(self):
        key = '{}{}'.format(define.TIFLASH_CLUSTER_HTTP_PORT, self.cur_store.address)
        val = conf.flash_conf.http_addr
        self.pd_client.etcd_client.update(key, val, max(conf.flash_conf.cluster_master_ttl, 300))

    def __init__(self, pd_client: PDClient, tidb_status_addr_list):
        self.logger = logging.getLogger('TiFlashManager')
        self.tidb_status_addr_list = tidb_status_addr_list
        self.pd_client = pd_client
        self.stores = {}
        self.cur_store = None
        self._update_cluster()

        self.state = [TiFlashClusterManager.ROLE_INIT, 0]
        self._try_refresh()
        self.ddl_global_schema_version = None
        self.ddl_global_schema_check_tso = None
        self.table_update()

    def _update_cluster(self):
        prev_stores = self.stores
        self.stores = {store_id: Store(store) for store_id, store in
                       self.pd_client.get_store_by_labels(define.TIFLASH_LABEL).items()}
        if self.stores != prev_stores and prev_stores:
            self.logger.info('Update all tiflash stores: from {} to {}'.format([k.inner for k in prev_stores.values()],
                                                                               [k.inner for k in self.stores.values()]))
        self.cur_store = self.compute_cur_store(self.stores)
        self._update_http_port()

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
        table_id = table['id']
        self.logger.info(
            'report to tidb: id {}, region_count {}, flash_region_count {}'.format(table_id, region_count,
                                                                                   flash_region_count))

        error_list = []
        for idx, address in enumerate(self.tidb_status_addr_list):
            try:
                r = util.post_http(
                    '{}/tiflash/replica'.format(address, PDClient.PD_API_PREFIX, PDClient.PD_API_VERSION),
                    {'id': table_id, define.REGION_COUNT: region_count,
                     define.TIFLASH_REGION_COUNT: flash_region_count})
                util.check_status_code(r)
                return
            except Exception as e:
                error_list.append((address, e))
                continue

        self.logger.error('can not report replica sync status to tidb: {}'.format(error_list))

    @wrap_try_get_lock
    def remove_rule(self, rule_id):
        self.pd_client.remove_rule(placement_rule.TIFLASH_GROUP_ID, rule_id)
        self.logger.info('Remove placement rule {}'.format(rule_id))

    def escape_table_update(self):
        ddl_global_schema_version, _ = self.pd_client.etcd_client.get(define.DDL_GLOBAL_SCHEMA_VERSION)

        last_handled_schema_version_tso, _ = self.pd_client.etcd_client.get(define.TIFLASH_LAST_HANDLED_SCHEMA_VERSION)

        if ddl_global_schema_version is None:
            ddl_global_schema_version = b'0'

        if last_handled_schema_version_tso is None:
            last_handled_schema_version_tso = b'%d%b%d' % (0, define.TIFLASH_LAST_HANDLED_SCHEMA_VERSION_TSO_SPLIT, 0)

        last_handled_schema_version, last_handled_schema_tso = last_handled_schema_version_tso.split(
            define.TIFLASH_LAST_HANDLED_SCHEMA_VERSION_TSO_SPLIT)

        cur_tso = int(time.time())

        if ddl_global_schema_version == last_handled_schema_version and int(
            last_handled_schema_tso) + define.TIFLASH_LAST_HANDLED_SCHEMA_TIME_OUT > cur_tso:
            return True

        self.ddl_global_schema_version = ddl_global_schema_version
        self.ddl_global_schema_check_tso = cur_tso

        return False

    @wrap_try_get_lock
    def table_update(self):
        if self.escape_table_update():
            return

        self._get_http_port_for_all_store()

        all_replica_available = True
        table_list = tidb_tools.db_flash_replica(self.tidb_status_addr_list)
        all_rules = self.pd_client.get_group_rules(placement_rule.TIFLASH_GROUP_ID)
        for table in table_list:
            from tikv_util import common

            table_id = table['id']
            st, ed = common.make_table_begin(table_id), common.make_table_end(table_id)
            start_key, end_key = st.to_bytes(), ed.to_bytes()
            start_key_hex, end_key_hex = st.to_pd_key(), ed.to_pd_key()
            self._check_and_make_rule(table, start_key_hex, end_key_hex, all_rules)

            if not table[define.AVAILABLE]:
                if table.get(define.PRIORITY, False):
                    self.pd_client.set_accelerate_schedule(start_key_hex, end_key_hex)
                    self.logger.info('try to accelerate pd schedule for table {}'.format(table_id))

                region_count, flash_region_count = self.compute_sync_data_process(table_id, start_key, end_key)
                self.report_to_tidb(table, region_count, flash_region_count)
                all_replica_available = False

        for rule in all_rules.values():
            self.remove_rule(rule.id)

        if all_replica_available:
            v = b'%b%b%d' % (self.ddl_global_schema_version, define.TIFLASH_LAST_HANDLED_SCHEMA_VERSION_TSO_SPLIT,
                             self.ddl_global_schema_check_tso)
            self.pd_client.etcd_client.put(define.TIFLASH_LAST_HANDLED_SCHEMA_VERSION, v)
            self.logger.info(
                'all replicas are available at global schema version {}'.format(int(self.ddl_global_schema_version)))


def main():
    flash_conf = conf.flash_conf
    parent_path = os.path.dirname(flash_conf.log_path)
    if not os.path.exists(parent_path):
        os.makedirs(parent_path)

    # keep at most 10G log files
    logging.basicConfig(
        handlers=[RotatingFileHandler(flash_conf.log_path, maxBytes=1024 * 1024 * 500, backupCount=5)],
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
