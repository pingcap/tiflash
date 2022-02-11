#!/usr/bin/python3
import argparse
import logging

import version

version_info = ''
for d in dir(version):
    if not d.startswith('__'):
        version_info += '{}: {}\n'.format(d, getattr(version, d))

parser = argparse.ArgumentParser(description='TiFlash Cluster Manager', formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('--version', '-v', help='show version', action='version', version=version_info)
parser.add_argument('--config', help='path of config file *.toml', required=True)
parser.add_argument('--log_level', help='log level', default='INFO', choices=['INFO', 'DEBUG', 'WARN'])
parser.add_argument('--check_online_update', help='check can do online rolling update for TiFlash', action='store_true')
parser.add_argument('--clean_pd_rules', help='clean all placement rules about tiflash in pd', action='store_true')

args = parser.parse_args()

import flash_tools

flash_conf = flash_tools.FlashConfig(args.config)
log_level = logging._nameToLevel.get(args.log_level)
