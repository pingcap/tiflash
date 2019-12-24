#!/bin/bash

is_force="$1"

if [[ "$is_force" == "true" ]]; then
  is_force="--force"
else
  is_force=""
fi

python3 setup.py build_ext --inplace ${is_force}
