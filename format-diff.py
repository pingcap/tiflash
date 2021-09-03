#!/usr/bin/python3
import argparse
import os
import subprocess
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
    parser.add_argument('--repo_path', help='path of tics repository',
                        default=os.path.dirname(os.path.abspath(__file__)))
    parser.add_argument('--suffix',
                        help='suffix of files to format, split by space', default=' '.join(default_suffix))
    parser.add_argument('--ignore_suffix', help='ignore files with suffix, split by space')
    parser.add_argument('--diff_from', help='commit hash/branch to check git diff', default='HEAD')
    parser.add_argument('--check_formatted', help='exit -1 if NOT formatted', action='store_true')
    parser.add_argument('--dump_diff_files_to', help='dump diff file names to specific path', default=None)

    args = parser.parse_args()
    default_suffix = args.suffix.strip().split(' ') if args.suffix else []
    ignore_suffix = args.ignore_suffix.strip().split(' ') if args.ignore_suffix else []
    tics_repo_path = args.repo_path
    if not os.path.isabs(tics_repo_path):
        raise Exception("path of repo should be absolute")
    assert tics_repo_path[-1] != '/'

    os.chdir(tics_repo_path)
    files_to_check = run_cmd('git diff HEAD --stat') if args.diff_from == 'HEAD' else run_cmd(
        'git diff {} --stat'.format(args.diff_from))
    files_to_check = [os.path.join(tics_repo_path, s.split()[0]) for s in files_to_check[:-1]]
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
        da = [e[len(tics_repo_path):] for e in files_to_format]
        json.dump({'files': da, 'repo': tics_repo_path}, open(args.dump_diff_files_to, 'w'))
        print('dump {} modified files info to {}'.format(len(da), args.dump_diff_files_to))

    if files_to_format:
        print('Files to format:\n  {}'.format('\n  '.join(files_to_format)))

        if args.check_formatted:
            cmd = 'clang-format -i {}'.format(' '.join(files_to_format))
            if subprocess.Popen(cmd, shell=True, cwd=tics_repo_path).wait():
                exit(-1)
            diff_res = run_cmd('git diff --stat')
            if diff_res:
                print('Error: found files NOT formatted')
                print('\n'.join(diff_res[:-1]))
                exit(-1)
            else:
                print("Format check passed")
        else:
            cmd = 'clang-format -i {}'.format(' '.join(files_to_format))
            subprocess.Popen(cmd, shell=True, cwd=tics_repo_path).wait()
            print("Finish code format")
    else:
        print('No file to format')


if __name__ == '__main__':
    main()
