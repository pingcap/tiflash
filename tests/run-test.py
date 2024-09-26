#!/usr/bin/env python3
# -*- coding:utf-8 -*-

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

import os
import re
import sys
import time
import datetime
from urllib.request import Request as UrlRequest
from urllib.error import HTTPError
from urllib.request import urlopen

CMD_PREFIX = '>> '
CMD_PREFIX_ALTER = '=> '
CMD_PREFIX_TIDB = 'mysql> '
CMD_PREFIX_TIDB_BINALY_AS_HEX = 'mysql_bin_as_hex> '
CMD_PREFIX_FUNC = 'func> '
RETURN_PREFIX = '#RETURN'
SLEEP_PREFIX = 'SLEEP '
TODO_PREFIX = '#TODO'
COMMENT_PREFIX = '#'
UNFINISHED_1_PREFIX = '\t'
UNFINISHED_2_PREFIX = '   '
WORD_PH = '{#WORD}'
LINE_PH = '{#LINE}'
REGEXP_MATCH = '{#REGEXP}'
CURL_TIDB_STATUS_PREFIX = 'curl_tidb> '
NO_UNESCAPE_SUFFIX = ' #NO_UNESCAPE'

# Some third-party module might output messge directly to stderr/stdin, use this list to ignore such outputs
IGNORED_CLIENT_OUTPUTS = ['<jemalloc>: Number of CPUs detected is not deterministic. Per-CPU arena disabled.']
verbose = False

def exec_func(cmd):
    cmd = cmd.strip()
    p = os.popen(cmd)
    output = p.readlines()
    err = p.close()
    return output, err

# translate string to avoid being escaped in shell environment
# we only need to consider '$', '`' and '\'
# ref: https://www.gnu.org/software/bash/manual/html_node/Double-Quotes.html
def to_unescaped_str(cmd):
    return cmd.replace('\\', '\\\\').replace('$', '\\$').replace('`', '\\`')

class Executor:
    def __init__(self, dbc):
        self.dbc = dbc

    def exe(self, cmd, unescape=True, binary_as_hex=False) -> list[str]:
        cmd = to_unescaped_str(cmd) if unescape else cmd
        if binary_as_hex:
            cmd = f'{self.dbc} "{cmd}" --binary-as-hex=true 2>&1'
        else:
            cmd = f'{self.dbc} "{cmd}" 2>&1'
        return exec_func(cmd)


class ShellFuncExecutor:
    def __init__(self, dbc):
        self.dbc = dbc

    def exe(self, cmd, unescape=False) -> list[str]:
        return exec_func(f'{cmd} "{self.dbc}" 2>&1')


class CurlTiDBExecutor:
    def __init__(self):
        self.tidb_status_addr = f'{os.getenv("tidb_server", "127.0.0.1")}:{os.getenv("tidb_status_port", 10080)}'

    def exe(self, context) -> list[str]:
        context = [e for e in context.split(' ') if e]
        method = context[0].upper()
        uri = f'http://{self.tidb_status_addr}/{context[1]}'
        request = UrlRequest(uri)
        request.get_method = lambda: method
        if method in ['POST', 'PUT']:
            request.data = context[2].encode()
        try:
            response = urlopen(request).read().strip()
            if response:
                response = response.decode('utf-8')
            return [response] if method == 'GET' and response else None, None
        except HTTPError as e:
            return [f'Error: {e}. Uri: {uri}'], e


def parse_line(line):
    words = [w.strip() for w in line.split("│") if w.strip() != ""]
    return "@".join(words)


def parse_table_parts(lines, fuzz):
    parts = set()
    if not fuzz:
        curr = []
        for line in lines:
            if line.startswith('┌'):
                if len(curr) != 0:
                    parts.add('\n'.join(curr))
                    curr = []
            curr.append(parse_line(line))
        if len(curr) != 0:
            parts.add('\n'.join(curr))
    else:
        for line in lines:
            if not line.startswith('┌') and not line.startswith('└'):
                line = parse_line(line)
                if line in parts:
                    line += '-extra'
                parts.add(line)
    return parts


def is_blank_char(c):
    return c in [' ', '\n', '\t']


def is_brace_char(c):
    return c in ['{', '[', '(', ')', ']', '}']


def is_break_char(c):
    return (c in [',', ';']) or is_brace_char(c) or is_blank_char(c)


def match_ph_word(line):
    i = 0
    while is_blank_char(line[i]):
        i += 1
    found = False
    while not is_break_char(line[i]):
        i += 1
        found = True
    return i if found else 0


# TODO: Support more place holders, eg: {#NUMBER}
def compare_line(line, template):
    if template.startswith(REGEXP_MATCH):
        return re.match(template[len(REGEXP_MATCH):], line) != None
    l = template.find(LINE_PH)
    if l >= 0:
        return True
    while True:
        i = template.find(WORD_PH)
        if i < 0:
            return line == template
        if line[:i] != template[:i]:
            return False
        j = match_ph_word(line[i:])
        if j == 0:
            return False
        template = template[i + len(WORD_PH):]
        line = line[i + j:]


class MySQLCompare:
    @staticmethod
    def parse_output_line(line):
        words = [w.strip() for w in line.split("\t") if w.strip() != ""]
        return "@".join(words)

    @staticmethod
    def parse_mysql_line(line):
        words = [w.strip() for w in line.split("|") if w.strip() != ""]
        return "@".join(words)

    @staticmethod
    def parse_mysql_outputs(outputs):
        results = set()
        for output_line in outputs:
            parsed_line = MySQLCompare.parse_output_line(output_line)
            while parsed_line in results:
                parsed_line += '-extra'
            results.add(parsed_line)
        return results

    @staticmethod
    def parse_excepted_outputs(outputs):
        results = set()
        for output_line in outputs:
            if not output_line.startswith('+'):
                parsed_line = MySQLCompare.parse_mysql_line(output_line)
                while parsed_line in results:
                    parsed_line += '-extra'
                results.add(parsed_line)
        return results

    @staticmethod
    def matched(outputs, matches):
        if len(outputs) == 0 and len(matches) == 0:
            return True
        is_table_parts = len(matches) > 0 and matches[0].startswith('+')
        if is_table_parts:
            a = MySQLCompare.parse_mysql_outputs(outputs)
            b = MySQLCompare.parse_excepted_outputs(matches)
            return a == b
        else:
            if len(outputs) > len(matches):
                return False
            for i in range(0, len(outputs)):
                if not compare_line(outputs[i], matches[i]):
                    return False
            for i in range(len(outputs), len(matches)):
                if not compare_line("", matches[i]):
                    return False
            return True


def matched(outputs, matches, fuzz):
    if len(outputs) == 0 and len(matches) == 0:
        return True

    is_table_parts = len(matches) > 0 and matches[0].startswith('┌')
    if is_table_parts:
        a = parse_table_parts(outputs, fuzz)
        b = parse_table_parts(matches, fuzz)
        return a == b
    else:
        if len(outputs) > len(matches):
            return False
        for i in range(0, len(outputs)):
            if not compare_line(outputs[i], matches[i]):
                return False
        for i in range(len(outputs), len(matches)):
            if not compare_line("", matches[i]):
                return False
        return True


class Matcher:
    def __init__(self, executor, executor_tidb, executor_func, executor_curl_tidb, fuzz):
        self.executor = executor
        self.executor_tidb = executor_tidb
        self.executor_func = executor_func
        self.executor_curl_tidb = executor_curl_tidb
        self.query_line_number = 0
        self.fuzz = fuzz
        self.query = None
        self.outputs = None
        self.matches = []
        self.is_mysql = False

    def on_line(self, line, line_number):
        if line.startswith(SLEEP_PREFIX):
            time.sleep(float(line[len(SLEEP_PREFIX):]))
        elif line.startswith(CMD_PREFIX_TIDB) or line.startswith(CMD_PREFIX_TIDB_BINALY_AS_HEX):
            unescape_flag = True
            if line.endswith(NO_UNESCAPE_SUFFIX):
                unescape_flag = False
                line = line[:-len(NO_UNESCAPE_SUFFIX)]
            if verbose: print(f'{datetime.datetime.now().strftime("%H:%M:%S.%f")} running {line}')
            if self.outputs != None and ((not self.is_mysql and not matched(self.outputs, self.matches, self.fuzz)) or (
                self.is_mysql and not MySQLCompare.matched(self.outputs, self.matches))):
                return False
            self.query_line_number = line_number
            self.is_mysql = True
            if line.startswith(CMD_PREFIX_TIDB_BINALY_AS_HEX):
                self.query = line[len(CMD_PREFIX_TIDB_BINALY_AS_HEX):]
            else:
                self.query = line[len(CMD_PREFIX_TIDB):]
            # for mysql commands ignore errors since they may be part of the test logic.
            self.outputs, _ = self.executor_tidb.exe(self.query, unescape_flag, line.startswith(CMD_PREFIX_TIDB_BINALY_AS_HEX))
            self.outputs = [x.strip() for x in self.outputs if len(x.strip()) != 0]
            self.matches = []
        elif line.startswith(CURL_TIDB_STATUS_PREFIX):
            if verbose: print(f'{datetime.datetime.now().strftime("%H:%M:%S.%f")} running {line}')
            if self.outputs != None and ((not self.is_mysql and not matched(self.outputs, self.matches, self.fuzz)) or (
                self.is_mysql and not MySQLCompare.matched(self.outputs, self.matches))):
                return False
            self.query_line_number = line_number
            self.is_mysql = True
            self.query = line[len(CURL_TIDB_STATUS_PREFIX):]
            self.outputs, err = self.executor_curl_tidb.exe(self.query)
            if err != None:
                return False
            self.matches = []
        elif line.startswith(CMD_PREFIX) or line.startswith(CMD_PREFIX_ALTER):
            if verbose: print(f'{datetime.datetime.now().strftime("%H:%M:%S.%f")} running {line}')
            if self.outputs != None and ((not self.is_mysql and not matched(self.outputs, self.matches, self.fuzz)) or (
                self.is_mysql and not MySQLCompare.matched(self.outputs, self.matches))):
                return False
            self.query_line_number = line_number
            self.is_mysql = False
            self.query = line[len(CMD_PREFIX):]
            # for commands ignore errors since they may be part of the test logic.
            self.outputs, _ = self.executor.exe(self.query)
            self.outputs = [x.strip() for x in self.outputs if len(x.strip()) != 0]
            for ignored_output in IGNORED_CLIENT_OUTPUTS:
                self.outputs = [x for x in self.outputs if x.find(ignored_output) < 0]
            self.matches = []
        elif line.startswith(CMD_PREFIX_FUNC):
            if verbose: print(f'{datetime.datetime.now().strftime("%H:%M:%S.%f")} running {line}')
            if self.outputs != None and ((not self.is_mysql and not matched(self.outputs, self.matches, self.fuzz)) or (
                self.is_mysql and not MySQLCompare.matched(self.outputs, self.matches))):
                return False
            self.query_line_number = line_number
            self.is_mysql = False
            self.query = line[len(CMD_PREFIX_FUNC):]
            self.outputs, err = self.executor_func.exe(self.query)
            self.outputs = [x.strip() for x in self.outputs]
            if err != None:
                return False
            self.outputs = []
            self.matches = []
        else:
            self.matches.append(line)
        return True

    def on_finish(self):
        if self.outputs != None and ((not self.is_mysql and not matched(self.outputs, self.matches, self.fuzz)) or (
            self.is_mysql and not MySQLCompare.matched(self.outputs, self.matches))):
            return False
        return True


def parse_exe_match(path, executor, executor_tidb, executor_func, executor_curl_tidb, fuzz):
    todos = []
    line_number = 0
    line_number_cached = 0
    with open(path) as file:
        matcher = Matcher(executor, executor_tidb, executor_func, executor_curl_tidb, fuzz)
        cached = None
        for origin in file:
            line_number += 1
            line = origin.strip()
            if line.startswith(RETURN_PREFIX):
                break
            if line.startswith(TODO_PREFIX):
                todos.append(line[len(TODO_PREFIX):].strip())
                continue
            if line.startswith(COMMENT_PREFIX) or len(line) == 0:
                continue
            if origin.startswith(UNFINISHED_1_PREFIX) or origin.startswith(UNFINISHED_2_PREFIX):
                if cached[-1] == ',':
                    cached += ' '
                cached += line
                continue
            if cached != None and not matcher.on_line(cached, line_number_cached):
                return False, matcher, todos
            cached = line
            line_number_cached = line_number
        if (cached != None and not matcher.on_line(cached, line_number)) or not matcher.on_finish():
            return False, matcher, todos
        return True, matcher, todos


def run():
    if len(sys.argv) not in (5, 6):
        print('usage: <bin> tiflash-client-cmd test-file-path fuzz-check tidb-client-cmd [verbose]')
        sys.exit(1)

    dbc = sys.argv[1]
    path = sys.argv[2]
    fuzz = (sys.argv[3] == 'true')
    mysql_client = sys.argv[4]
    global verbose
    if len(sys.argv) == 6:
        verbose = (sys.argv[5] == 'true')
    if verbose: print(f'parsing file: `{path}`')
    matched, matcher, todos = parse_exe_match(path, Executor(dbc), Executor(mysql_client),
                                              ShellFuncExecutor(mysql_client),
                                              CurlTiDBExecutor(),
                                              fuzz,
                                              )

    def display(lines):
        if len(lines) == 0:
            print(' ' * 4 + '<nothing>')
        else:
            for it in lines:
                print(' ' * 4 + it)

    if not matched:
        print(f'  File: {path}')
        print(f'  Error line: {matcher.query_line_number}')
        print(f'  Error: {matcher.query}')
        print('  Result:')
        display(matcher.outputs)
        print('  Expected:')
        display(matcher.matches)
        sys.exit(1)
    if todos:
        print('  TODO:')
        for it in todos:
            print(' ' * 4 + it)


def main():
    try:
        run()
    except KeyboardInterrupt:
        print('KeyboardInterrupted')
        sys.exit(1)


if __name__ == '__main__':
    main()
