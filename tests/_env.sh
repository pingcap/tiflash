#!/bin/bash

# Executable path

if [ `uname` == "Darwin" ]; then
	export build_dir="../../build_clang"
else
	export build_dir="../../build"
fi

export storage_bin="$build_dir/dbms/src/Server/theflash"

# Serve config for launching
export storage_server_config="../../running/config/config.xml"

# Server address for connecting
export storage_server="127.0.0.1"

# Default database for scripts
export storage_db="default"

# Setup running env vars
source ../../_vars.sh
setup_dylib_path
