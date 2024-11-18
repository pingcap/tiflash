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
import signal
import sys
import time
import logging
import types
import subprocess

logger = None


def get_tz_offset():
    import datetime
    now_stamp = time.time()
    local_time = datetime.datetime.fromtimestamp(now_stamp)
    utc_time = datetime.datetime.utcfromtimestamp(now_stamp)
    offset = local_time - utc_time
    total_seconds = offset.total_seconds()
    flag = '+'
    if total_seconds < 0:
        flag = '-'
        total_seconds = -total_seconds
    mm, ss = divmod(total_seconds, 60)
    hh, mm = divmod(mm, 60)
    tz_offset = "%s%02d:%02d" % (flag, hh, mm)
    return tz_offset


def init_logger():
    global logger

    tz_offset = get_tz_offset()

    orig_record_factory = logging.getLogRecordFactory()
    log_colors = {
        logging.DEBUG: "\033[1;34m",  # blue
        logging.INFO: "\033[1;32m",  # green
        logging.WARNING: "\033[1;35m",  # magenta
        logging.ERROR: "\033[1;31m",  # red
        logging.CRITICAL: "\033[1;41m",  # red reverted
    }

    def get_message(ori):
        msg = str(ori.msg)
        if ori.args:
            msg = msg % ori.args
        msg = "{}{}{}".format(log_colors[ori.levelno], msg, "\033[0m")
        return msg

    def record_factory(*args, **kwargs):
        record = orig_record_factory(*args, **kwargs)
        record.getMessage = types.MethodType(get_message, record)
        return record

    logging.setLogRecordFactory(record_factory)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        fmt=logging.Formatter('[%(asctime)s.%(msecs)03d {}][%(levelname)s][%(message)s]'.format(tz_offset),
                              datefmt='%Y/%m/%d %H:%M:%S'))
    root.addHandler(handler)
    logger = root


init_logger()


def wrap_run_time(func):
    def wrap_func(*args, **kwargs):
        bg = time.time()
        r = func(*args, **kwargs)
        logger.debug('Time cost {:.3f}s'.format(time.time() - bg))
        return r

    return wrap_func


@wrap_run_time
def run_cmd(cmd):
    logger.debug("RUN CMD:\n{}\n".format(' '.join(cmd)))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    # stderr.decode('utf-8')
    return stdout, stderr, proc.returncode


class Runner:
    def __init__(self):
        usage = """
1. compile TiFlash with cmake option `-DENABLE_LLVM_PGO=ON -DENABLE_LLVM_PGO_USE_SAMPLE=ON`
2. compile https://github.com/google/autofdo and get binary `create_llvm_prof` for converting perf data to llvm profile data
3. start TiFlash process and get `<pid>`
4. prepare workload scripts file
5. run `python3 perf-tpch.py --perf --pid <pid> --workload <workload-scripts-path> --convert-llvm --convert-tool <create_llvm_prof-path> --binary <tiflash-bianry-path>`
6. get llvm perf file(`tiflash.llvm.code.prof` by default)
7. compile TiFlash with env `TIFLASH_LLVM_PROFDATA=<output-llvm-prof>` and cmake option `-DENABLE_LLVM_PGO=ON -DENABLE_LLVM_PGO_USE_SAMPLE=ON`
8. re-run workload and compare result
"""
        parser = argparse.ArgumentParser(
            description="Auto FDO tools", formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            usage=usage,
            add_help=False)
        self.parser = parser

        parser.add_argument(
            '--perf', help='run perf with workload', action='store_true')
        parser.add_argument(
            '--convert-llvm', help='convert linux perf data to llvm profile data', action='store_true')
        parser.add_argument(
            '--output', help='output file of perf data')
        parser.add_argument(
            '-h', '--help',
            action='store_true',
            help=('show this help message and exit'))

        self.args, rem_args = parser.parse_known_args()

        if self.args.perf:
            parser.add_argument(
                '--workload', help='absolute path of workload script', required=True)
            parser.add_argument(
                '--pid', help='pid of TiFlash process', required=True)

        if self.args.convert_llvm:
            parser.add_argument(
                '--convert-tool', help='tool to conver linux perf data to llvm profile data', required=True)
            parser.add_argument(
                '--input-perf-file', help='input linux perf data file path', required=False if self.args.perf else True)
            parser.add_argument(
                '--binary', help='binary to run workload', required=True)
            parser.add_argument(
                '--output-llvm-prof', help='output llvm profile data path', default='tiflash.llvm.code.prof')

        self._show_help()

        parser.parse_args(rem_args, namespace=self.args)

        self.linux_perf_data = None

    def _show_help(self):
        if self.args.help:
            self.parser.print_help()
            self.parser.exit()

    def run(self):
        if self.args.perf:
            self.run_perf()
        if self.args.convert_llvm:
            self.convert_llvm_perf()

    def convert_llvm_perf(self):
        if self.args.input_perf_file is None:
            assert self.linux_perf_data
            self.args.input_perf_file = self.linux_perf_data
        assert os.path.isfile(self.args.input_perf_file)

        self.args.output_llvm_prof = 'tiflash.llvm.code.prof'
        self.args.binary = os.path.realpath(self.args.binary)
        assert os.path.isfile(self.args.binary)

        logger.info('start to convert linux perf data `{}` to llvm profile data `{}`'.format(
            self.args.input_perf_file, self.args.output_llvm_prof))
        stdout, stderr, e = run_cmd([self.args.convert_tool, '--profile', '{}'.format(self.args.input_perf_file),
                                     '--binary', "{}".format(self.args.binary),
                                     '--out', '{}'.format(self.args.output_llvm_prof)])
        logger.info(
            'finish convert. stdout `{}`, stderr `{}`'.format(stdout.decode('utf-8'), stderr.decode('utf-8')))
        assert e == 0

    def run_perf(self):
        pid = self.args.pid
        output = 'tiflash.perf.data' if self.args.output is None else self.args.output
        logger.info('using output file `{}`'.format(output))

        def workload():
            # git clone git@github.com:pingcap/go-tpc.git
            # cd go-tpc
            # make build
            # bin/go-tpc tpch run --queries q1 --host {} -P {} --db {} --count 1
            logger.info('start to run workload `{}`'.format(
                self.args.workload))
            stdout, stderr, err = run_cmd([self.args.workload])
            logger.info('finish workload `{}`. stdout `{}`, stderr `{}`'.format(
                self.args.workload, stdout.decode('utf-8'), stderr.decode('utf-8')))
            assert err == 0
        perf_cmd = ["perf", "record", "-p", "{}".format(
            pid), "-e", "cycles:up", "-j", "any,u", "-a", "-o", "{}".format(output)]
        logger.info("start perf with cmd `{}`".format(' '.join(perf_cmd)))
        perf_proc = subprocess.Popen(
            perf_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #
        workload()
        #
        perf_proc.send_signal(signal.SIGTERM)
        stdout, stderr = perf_proc.communicate()
        logger.info(
            "stop perf. stdout `{}`, stderr `{}`".format(stdout.decode('utf-8'), stderr.decode('utf-8')))
        _ = perf_proc.wait()
        # check file exits
        with open(output, 'r') as f:
            f.close()
        self.linux_perf_data = output


def main():
    Runner().run()


if __name__ == '__main__':
    main()
