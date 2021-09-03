#!/usr/bin/python3
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
    parser.add_argument('--includes', help='external include directories, split by `,`', required=True)
    parser.add_argument('--load_diff_files_from', help='load diff file names from specific path', default=None)
    args = parser.parse_args()

    includes = args.includes.split(',')
    print('start to fix `{}` by adding external include directories {}'.format(args.file_path, includes))
    includes = ' '.join(['-I{}'.format(a) for a in includes])
    data = json.load(open(args.file_path, 'r'))

    if args.load_diff_files_from:
        data = check_files(data, args.load_diff_files_from)

    for line in data:
        line['command'] += ' ' + includes
    json.dump(data, open(args.file_path, 'w'), indent=4)
    print('process {} elements'.format(len(data)))


if __name__ == '__main__':
    main()
