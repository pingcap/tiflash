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
import os
import subprocess
from shutil import which
from os import path

import json


def run_cmd(cmd, show_cmd=False):
    res = os.popen(cmd).readlines()
    if show_cmd:
        print("RUN CMD: {}".format(cmd))
    return res


def main():
    default_suffix = ['.cpp', '.h', '.cc', '.hpp']
    parser = argparse.ArgumentParser(description='TiFlash Code Format',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--repo_path', help='path of tiflash repository',
                        default=os.path.dirname(os.path.abspath(__file__)))
    parser.add_argument('--suffix',
                        help='suffix of files to format, split by space', default=' '.join(default_suffix))
    parser.add_argument('--ignore_suffix',
                        help='ignore files with suffix, split by space')
    parser.add_argument(
        '--diff_from', help='commit hash/branch to check git diff', default='HEAD')
    parser.add_argument('--check_formatted',
                        help='exit -1 if NOT formatted', action='store_true')
    parser.add_argument('--dump_diff_files_to',
                        help='dump diff file names to specific path', default=None)

    args = parser.parse_args()
    default_suffix = args.suffix.strip().split(' ') if args.suffix else []
    ignore_suffix = args.ignore_suffix.strip().split(
        ' ') if args.ignore_suffix else []
    tiflash_repo_path = args.repo_path
    if not os.path.isabs(tiflash_repo_path):
        raise Exception("path of repo should be absolute")
    assert tiflash_repo_path[-1] != '/'

    os.chdir(tiflash_repo_path)
    files_to_check = run_cmd('git diff HEAD --name-only') if args.diff_from == 'HEAD' else run_cmd(
        'git diff {} --name-only'.format(args.diff_from))
    files_to_check = [os.path.join(tiflash_repo_path, s.strip())
                      for s in files_to_check]
    files_to_format = []
    for f in files_to_check:
        if not any([f.endswith(e) for e in default_suffix]):
            continue
        if any([f.endswith(e) for e in ignore_suffix]):
            continue
        file_path = f
        if not path.exists(file_path):
            continue
        if ' ' in file_path:
            print('file {} can not be formatted'.format(file_path))
            continue
        files_to_format.append(file_path)

    if args.dump_diff_files_to:
        da = [e[len(tiflash_repo_path):] for e in files_to_format]
        json.dump({'files': da, 'repo': tiflash_repo_path},
                  open(args.dump_diff_files_to, 'w'))
        print('dump {} modified files info to {}'.format(
            len(da), args.dump_diff_files_to))

    if files_to_format:
        print('Files to format:\n  {}'.format('\n  '.join(files_to_format)))
        clang_format_cmd = 'clang-format-17'
        clang_format_15_cmd = 'clang-format-15'
        if which(clang_format_cmd) is None:
            if which(clang_format_15_cmd) is not None:
                clang_format_cmd = clang_format_15_cmd
            else:
                clang_format_cmd = 'clang-format'
        for file in files_to_format:
            cmd = clang_format_cmd + ' -i {}'.format(file)
            if subprocess.Popen(cmd, shell=True, cwd=tiflash_repo_path).wait():
                exit(-1)

        if args.check_formatted:
            diff_res = run_cmd('git diff --name-only')
            files_not_in_contrib = [f for f in diff_res if not f.startswith('contrib')]
            files_contrib = [f for f in diff_res if f.startswith('contrib')]
            if files_not_in_contrib:
                print('')
                print('Error: found files NOT formatted')
                print(''.join(files_not_in_contrib))
                exit(-1)
            elif files_contrib:
                print('')
                print('Warn: found contrib changed')
                print(''.join(files_contrib))
                print('')
                print(''.join(run_cmd('git status')))
            else:
                print("Format check passed")
        else:
            print("Finish code format")
    else:
        print('No file to format')


if __name__ == '__main__':
    main()
