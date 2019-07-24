#!/bin/bash

# Executable path

if [ `uname` == "Darwin" ]; then
	export storage_bin="../../build_clang/dbms/src/Server/theflash"
else
	export storage_bin="../../build/dbms/src/Server/theflash"
fi

# Serve config for launching
export storage_server_config="../../running/config/config.xml"

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

# Setup running env vars
source ../../_vars.sh
setup_dylib_path
