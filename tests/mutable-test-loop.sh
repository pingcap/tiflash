#!/bin/bash

set -eu
for ((i = 0; i < 1000; i++)); do
	./run-test.sh mutable-test
done
