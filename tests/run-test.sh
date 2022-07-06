#!/bin/bash
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


function wait_table()
{
	python2 wait-table.py "$@"; return $?
}
export -f wait_table

function run_file()
{
	local dbc="$1"
	local path="$2"
	local continue_on_error="$3"
	local fuzz="$4"
	local skip_raw_test="$5"
	local mysql_client="$6"
	local verbose="$7"

	local ext=${path##*.}

	if [ "$ext" == "test" ]; then
		python2 run-test.py "$dbc" "$path" "$fuzz" "$mysql_client" "$verbose"
	else
		if [ "$ext" == "visual" ]; then
			python2 run-test-gen-from-visual.py "$path" "$skip_raw_test" "$verbose"
			if [ $? != 0 ]; then
				echo "Generate test files failed: $file" >&2
				exit 1
			fi
			run_dir "$dbc" "$path.test" "$continue_on_error" "$fuzz" "$skip_raw_test" "$mysql_client" "$verbose"
		fi
	fi

	if [ $? == 0 ]; then
		echo $path: OK
	else
		echo $path: Failed
		if [ "$continue_on_error" != "true" ]; then
			exit 1
		fi
	fi
}

function run_dir()
{
	local dbc="$1"
	local path="$2"
	local continue_on_error="$3"
	local fuzz="$4"
	local skip_raw_test="$5"
	local mysql_client="$6"
	local verbose="$7"

	find "$path" -maxdepth 1 -name "*.test" -type f | sort | while read file; do
		if [ -f "$file" ]; then
			run_file "$dbc" "$file" "$continue_on_error" "$fuzz" "$skip_raw_test" "$mysql_client" "$verbose"
		fi
	done

	if [ $? != 0 ]; then
		exit 1
	fi

	find "$path" -maxdepth 1 -type d | sort -r | while read dir; do
		if [ -d "$dir" ] && [ "$dir" != "$path" ]; then
			run_dir "$dbc" "$dir" "$continue_on_error" "$fuzz" "$skip_raw_test" "$mysql_client" "$verbose"
		fi
	done

	if [ $? != 0 ]; then
		exit 1
	fi
}

function run_path()
{
	local dbc="$1"
	local path="$2"
	local continue_on_error="$3"
	local fuzz="$4"
	local skip_raw_test="$5"
	local mysql_client="$6"
	local verbose="$7"

	if [ -f "$path" ]; then
		run_file "$dbc" "$path" "$continue_on_error" "$fuzz" "$skip_raw_test" "$mysql_client" "$verbose"
	else
		if [ -d "$path" ]; then
			run_dir "$dbc" "$path" "$continue_on_error" "$fuzz" "$skip_raw_test" "$mysql_client" "$verbose"
		else
			echo "error: $path not file nor dir." >&2
			exit 1
		fi
	fi
}

set -e

target="$1"
fullstack="$2"
fuzz="$3"
skip_raw_test="$4"
debug="$5"
continue_on_error="$6"
dbc="$7"
verbose="${verbose:-8}"

source ./_env.sh

if [ -z "$target" ]; then
	target="delta-merge-test"
fi

if [ -z "$debug" ]; then
	debug="false"
fi

if [ -z "$fuzz" ]; then
	fuzz="true"
fi

if [ -z "$skip_raw_test" ]; then
	skip_raw_test="true"
fi

if [ -z "$dbc" ]; then
	if [ "$debug" != "false" ] && [ "$debug" != "0" ]; then
		debug="--stacktrace"
	fi
	dbc="DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH $storage_bin client --host $storage_server --port $storage_port -d $storage_db $debug -f PrettyCompactNoEscapes --query"
fi

if [ -z "$verbose" ]; then
    verbose="false"
fi

if [ -z "$continue_on_error" ]; then
	continue_on_error="false"
fi

if [ -z "fullstack" ]; then
	fullstack="false"
fi

"$storage_bin" client --host="$storage_server" --port="$storage_port" --query="create database if not exists $storage_db"
if [ $? != 0 ]; then
	echo "create database '"$storage_db"' failed" >&2
	exit 1
fi

mysql_client="mysql -u root -P $tidb_port -h $tidb_server -e"

if [ "$fullstack" = true ]; then
    mysql -u root -P $tidb_port -h $tidb_server -e "create database if not exists $tidb_db"
    sleep 10
    if [ $? != 0 ]; then
        echo "create database '"$tidb_db"' failed" >&2
        exit 1
    fi
    python2 generate-fullstack-test.py "$tidb_db" "$tidb_table"
fi

run_path "$dbc" "$target" "$continue_on_error" "$fuzz" "$skip_raw_test" "$mysql_client" "$verbose"
