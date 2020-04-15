#!/bin/bash

set -xe

# Create lcov report
lcov --capture --directory ./gcov --output-file coverage.info
lcov --remove coverage.info '/usr/*' '*/tics/contrib/*' '*/tics/libs/*' --output-file coverage.info # filter system files and third-party libraries
lcov --list coverage.info # debug info
# Uploading report to CodeCov
bash <(curl -s https://codecov.io/bash) -f coverage.info -t 256605ff-7740-4020-a053-2ce702ff4109 || echo "Codecov did not collect coverage reports"
