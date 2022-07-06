#!/bin/bash

# Executable path

if [ `uname` == "Darwin" ]; then
    export build_dir="../../build_clang"
else
    export build_dir="../../build"
fi

export storage_bin="$build_dir/dbms/src/Server/tiflash"

# Server address for connecting
export storage_server="127.0.0.1"

# Server port for connecting
export storage_port="9000"

# Default database for scripts
export storage_db="default"

# TiDB address
export tidb_server="127.0.0.1"

# TiDB port
export tidb_port="4000"

# TiDB default database
export tidb_db="test"

# TiDB default table
export tidb_table="t"

# Whether run scripts with verbose output
export verbose="false"
# export verbose="true"

# Setup running env vars
#source ../../_vars.sh
#setup_dylib_path
