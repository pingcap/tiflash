#!/bin/bash
set -x

cd "$(dirname "$0")"

export ENABLE_NEXT_GEN=1
export storage_bin=../cmake-build-debug-ng/dbms/src/Server/tiflash
export storage_port=5035
export tidb_server=10.2.12.81
export tidb_port=8031
export continue_on_error=true
export verbose=true

tests=(
	# fullstack-test/sample.test
	#fullstack-test-index
	# fullstack-test-next-gen/placement
	# fullstack-test2/clustered_index
	# fullstack-test2/dml
	# fullstack-test2/variables
	# fullstack-test2/mpp
	# fullstack-test/expr
	# fullstack-test/mpp
	fullstack-test2/ddl/alter_column_enum.test
)

failed=0
for test in "${tests[@]}"; do
	./run-test.sh "$test" || failed=$((failed + 1))
done

if [ "$failed" -gt 0 ]; then
	echo "Total failed test suites: $failed" >&2
	exit 1
fi
