#!/usr/bin/python3
import logging
import argparse
import version

version_info = ''
for d in dir(version):
    if not d.startswith('__'):
        version_info += '{}: {}\n'.format(d, getattr(version, d))

parser = argparse.ArgumentParser(description='TiFlash Cluster Manager', formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('--version', '-v', help='show version', action='version', version=version_info)
parser.add_argument('--config', help='path of config file *.toml', required=True)
parser.add_argument('--log_level', help='log level', default='INFO', choices=['INFO', 'DEBUG', 'WARN'])

args = parser.parse_args()

import flash_tools

flash_conf = flash_tools.FlashConfig(args.config)
log_level = logging._nameToLevel.get(args.log_level)
