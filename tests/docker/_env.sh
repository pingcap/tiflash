#!/bin/bash

export build_dir="/tiflash"

# Executable path
export storage_bin="$build_dir/tiflash"

# Server address for connecting
export storage_server="127.0.0.1"

# Server port for connecting
export storage_port="9000"

# Default database for scripts
export storage_db="default"

# TiDB address
export tidb_server="tidb0"

# TiDB port
export tidb_port="4000"

# TiDB default database
export tidb_db="test"

# TiDB default table
export tidb_table="t"
