#!/usr/bin/env python
# Copyright 2023 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function

import os
import sys

class UsageInfo(object):
    def __init__(self, inode_used, bytes_used):
        self.inode = inode_used
        self.bytes = bytes_used
    def merge(self, rhs):
        self.inode += rhs.inode
        self.bytes += rhs.bytes
    def __repr__(self):
        return '{{"inode": {}, "bytes": {}}}'.format(self.inode, self.bytes)

def collect(root_path):
    info = {}
    for p in os.listdir(root_path):
        full_sub_path = os.path.join(root_path, p)
        if os.path.isdir(full_sub_path):
            sub_path_infos = collect(full_sub_path)
            sub_dir_info = UsageInfo(1, 0) # directory itself takes 1 inode
            for p in sub_path_infos:
                sub_dir_info.merge(sub_path_infos[p])
            info[full_sub_path] = sub_dir_info
            print('{{"path": {}, "info": {}}}'.format(full_sub_path, info[full_sub_path]))
            continue
        sub_path_info = UsageInfo(1, os.path.getsize(full_sub_path))
        info[full_sub_path] = sub_path_info
        # print(full_sub_path, info[full_sub_path])
    return info

def main(root_path):
    sub_path_infos = collect(root_path)
    sub_dir_info = UsageInfo(1, 0) # directory itself takes 1 inode
    for p in sub_path_infos:
        sub_dir_info.merge(sub_path_infos[p])
    print('{{"path": {}, "info": {}}}'.format(root_path, sub_dir_info))

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: {} TIFLASH_DATA_DIR'.format(sys.argv[0]))
        sys.exit(1)

    root_path = sys.argv[1]
    # root_path = '/DATA/disk1/jaysonhuang/clusters/tiflash-5020/data'
    main(root_path)
