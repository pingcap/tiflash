#!/usr/bin/python3
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

import argparse
import json
import os


def check_files(data, load_diff_files_from):
    data_map = {e['file']: e for e in data}
    remain = []
    try:
        d = json.load(open(load_diff_files_from, 'r'))
    except Exception:
        return data
    files = d['files']
    repo = os.path.abspath(os.path.dirname(os.path.abspath(__file__)) + '/../../')
    for e in files:
        e = '{}{}'.format(repo, e)
        e = data_map.get(e)
        if e:
            remain.append(e)
    print('original {} files, remain {} files for static analysis'.format(len(data), len(remain)))
    return remain


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--file_path', help='path of `compile_commands.json`', required=True)
    parser.add_argument('--load_diff_files_from', help='load diff file names from specific path', default=None)
    args = parser.parse_args()
    data = json.load(open(args.file_path, 'r'))

    if args.load_diff_files_from:
        data = check_files(data, args.load_diff_files_from)
    json.dump(data, open(args.file_path, 'w'), indent=4)
    print('process {} elements'.format(len(data)))
    print('{}'.format(json.dumps(data, indent=4)))


if __name__ == '__main__':
    main()
