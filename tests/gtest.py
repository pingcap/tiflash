# Copyright 2022 PingCAP, Ltd.
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

"""
This script filters out log messages for successful test cases, only keep
those from error test cases.
"""

import subprocess
import sys
import os
import optparse
import json

if sys.version_info >= (3, 0):
    stdout = sys.stdout.buffer
else:
    stdout = sys.stdout


class TestRunner(object):

    TEST_MANIFEST_FILE = 'tests.json'

    GTEST_PREFIX_BEGIN = b'[ RUN      ]'
    GTEST_PREFIX_FAILED = b'[  FAILED  ]'
    GTEST_PREFIX_OK = b'[       OK ]'

    def __init__(self, cmd):
        self.cmd = cmd[:]
        self.n_tests_all = 0
        self.n_tests_disabled = 0
        self.n_tests_run = 0
        self.n_tests_fail = 0
        self.n_tests_ok = 0
        self.is_in_test_case = False
        self.buffered_outputs = []

    def discover_tests(self):
        """
        Discover how many number of tests we need to run.
        This allows us to print a friendly test progress.
        """

        try:
            os.remove(self.TEST_MANIFEST_FILE)
        except FileNotFoundError:
            pass

        full_cmd = self.cmd[:]
        full_cmd.extend(
            ['--gtest_list_tests', '--gtest_output=json:{}'.format(self.TEST_MANIFEST_FILE)])
        FNULL = open(os.devnull, 'w')
        retcode = subprocess.call(full_cmd,
                                  stdout=FNULL,
                                  stderr=subprocess.STDOUT,
                                  close_fds=True)
        if retcode != 0:
            raise Exception('Execute {}: Failed to discover tests, return code = {}'.format(
                ' '.join(full_cmd),
                retcode))

        with open(self.TEST_MANIFEST_FILE, 'r') as manifest_file:
            manifest = json.load(manifest_file)
        self.n_tests_all = 0
        for test_suite in manifest['testsuites']:
            for test_case in test_suite['testsuite']:
                # TODO: Maybe user specifies to run disabled test as well
                if test_case['name'].startswith('DISABLED_'):
                    self.n_tests_disabled += 1
                else:
                    self.n_tests_all += 1

    def get_print_prefix(self):
        return "[{:>5}/{:>5}] ".format(self.n_tests_run,
                                       self.n_tests_all).encode()

    def print_outputs(self):
        for line in self.buffered_outputs:
            stdout.write(line)
            stdout.write(b'\n')
        stdout.flush()

    def print_final_statistics(self):
        print('{} summary: all {} tests, {} ok, {} failed, {} disabled'.format(
            ' '.join(self.cmd),
            self.n_tests_all,
            self.n_tests_ok,
            self.n_tests_fail,
            self.n_tests_disabled))

    def run(self):
        print('{} running...'.format(' '.join(self.cmd)))

        self.discover_tests()

        p = subprocess.Popen(self.cmd,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        try:
            for this_line in iter(p.stdout.readline, b''):
                this_line = this_line[:-1]
                if this_line.startswith(self.GTEST_PREFIX_BEGIN):
                    if self.is_in_test_case:
                        # In case of output format is changed, let's throw something.
                        self.print_outputs()
                        raise Exception('Unexpected test start tag')
                    self.n_tests_run += 1
                    self.is_in_test_case = True
                    self.buffered_outputs = [
                        self.get_print_prefix() + this_line]
                elif this_line.startswith(self.GTEST_PREFIX_OK):
                    if not self.is_in_test_case:
                        # In case of output format is changed, let's throw something.
                        self.print_outputs()
                        raise Exception('Unexpected test ok tag')
                    self.n_tests_ok += 1
                    self.is_in_test_case = False
                    self.buffered_outputs = [
                        self.get_print_prefix() + this_line]
                    self.print_outputs()
                elif self.is_in_test_case and this_line.startswith(self.GTEST_PREFIX_FAILED):
                    self.n_tests_fail += 1
                    self.is_in_test_case = False
                    self.buffered_outputs.append(
                        self.get_print_prefix() + this_line)
                    self.print_outputs()
                elif self.is_in_test_case:
                    self.buffered_outputs.append(this_line)
                else:
                    # Just ignore outputs generated outside a test case
                    pass

            p.wait()

            # We didn't properly receive test end prefixes.
            # Maybe test suite is crashed. So just print buffered outputs.
            if self.is_in_test_case:
                self.print_outputs()

            self.print_final_statistics()
            return p.returncode

        except KeyboardInterrupt:
            print("Test interrupted")
            try:
                p.kill()
            except OSError:
                pass
            p.wait()

            self.print_final_statistics()
            return 1


def default_options_parser():
    parser = optparse.OptionParser(
        usage='usage: %prog binary [binary ...] -- [additional args]')
    return parser


def main():
    # Extract anything after `--`.
    additional_args = []
    for i in range(len(sys.argv)):
        if sys.argv[i] == '--':
            additional_args = sys.argv[i + 1:]
            sys.argv = sys.argv[:i]
            break

    parser = default_options_parser()
    (options, binaries) = parser.parse_args()
    # Currently options is unused.

    if len(binaries) == 0:
        parser.print_usage()
        sys.exit(1)

    exit_code = 0
    for binary in binaries:
        cmd = [binary]
        cmd.extend(additional_args)
        this_code = TestRunner(cmd).run()
        if this_code != 0 and exit_code == 0:
            exit_code = this_code

    return exit_code


if __name__ == '__main__':
    sys.exit(main())
