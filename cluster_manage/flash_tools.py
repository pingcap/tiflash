#!/usr/bin/python3
import toml

import util


class FlashConfig:

    def __init__(self, file_path):
        self.conf_file_path = file_path
        self.conf_toml = toml.load(self.conf_file_path, _dict=dict)
        self.pd_addrs = util.compute_addr_list(self.conf_toml['raft']['pd_addr'])
        if 'http_port' in self.conf_toml:
            self.http_port = self.conf_toml['http_port']
        else:
            self.http_port = self.conf_toml['https_port']

        p = self.conf_toml['flash']
        service_addr = p['service_addr']
        if p['proxy'].get('config') is not None:
            proxy_toml = toml.load(p['proxy']['config'], _dict=dict)
            service_addr = proxy_toml.get('server', {}).get('engine-addr', service_addr)
        service_addr = p['proxy'].get('engine-addr', service_addr)
        service_addr = service_addr.strip()
        host, port = [e.strip() for e in service_addr.split(':')]
        self.service_addr = '{}:{}'.format(host, port)
        self.http_addr = '{}:{}'.format(host, self.http_port)
        self.tidb_status_addr = util.compute_addr_list(p['tidb_status_addr'])
        flash_cluster = p.get('flash_cluster', {})
        self.cluster_master_ttl = flash_cluster.get('master_ttl', 60)
        self.cluster_refresh_interval = min(
            int(flash_cluster.get('refresh_interval', 20)), self.cluster_master_ttl)
        self.update_rule_interval = int(flash_cluster.get('update_rule_interval', 10))
        self.log_path = flash_cluster.get('log', '{}/flash_cluster_manager.log'.format(self.conf_toml.get('tmp_path', '/tmp')))
        self.max_time_out = self.cluster_master_ttl

        self.enable_tls = False
        if 'security' in self.conf_toml:
            security = self.conf_toml['security']
            if 'ca_path' in security:
                self.ca_path = security['ca_path']
                self.key_path = security['key_path']
                self.cert_path = security['cert_path']
                self.enable_tls = True


def main():
    pass


if __name__ == '__main__':
    main()
