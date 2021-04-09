#!/bin/bash
set -euo pipefail

rm -rf cpp/dtpb && mkdir -p cpp/dtpb
echo "generate cpp code..."
protoc -I=./proto --cpp_out=./cpp/dtpb ./proto/*.proto
