#!/usr/bin/python3
import toml

import util


class FlashConfig:

    def __init__(self, file_path):
        self.conf_file_path = file_path
        self.conf_toml = toml.load(self.conf_file_path, _dict=dict)
        self.pd_addrs = util.compute_addr_list(self.conf_toml['raft']['pd_addr'])
        self.http_port = self.conf_toml['http_port']
        tmp_path = self.conf_toml['tmp_path']

        p = self.conf_toml['flash']
        service_addr = p['service_addr']
        host, port = [e.strip() for e in service_addr.split(':')]
        self.service_addr = '{}:{}'.format(host, port)
        self.http_addr = '{}:{}'.format(host, self.http_port)
        self.tidb_status_addr = util.compute_addr_list(p['tidb_status_addr'])
        flash_cluster = p['flash_cluster']
        self.cluster_master_ttl = flash_cluster['master_ttl']
        self.cluster_refresh_interval = min(
            int(flash_cluster['refresh_interval']), self.cluster_master_ttl)
        self.update_rule_interval = int(flash_cluster['update_rule_interval'])
        self.log_path = flash_cluster.get('log', '{}/flash_cluster_manager.log'.format(tmp_path))



def main():
    pass


if __name__ == '__main__':
    main()
