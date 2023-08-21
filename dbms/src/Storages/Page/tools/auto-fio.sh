#!/bin/sh
# Copyright 2023 PingCAP, Inc.
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

# fio test for EBS(with filesystem) write/randwrite test.

if [ $# -lt 1 ]; then
    echo 'Usage: "./auto-fio.sh <path>" '
    echo '  output for test case 1: t1_{wr}_ncore{cores}_depth{iodepth}_fio.output'
    echo '  output for test case 2: t2_ncore{core}_depth{iodepth}_with_{rand or seqwrite}_fio.output'
    exit 1
fi

# test case 1 
call_fio_cmd()
{
    fio --directory=$1 \
    --filename=fix_fio_test_file\
    --direct=1 \
    --rw=$2 \
    --norandommap \
    --randrepeat=0 \
    --ioengine=psync \
    --blocksize_range=4k-1M \
    --iodepth=$4 \
    --numjobs=$3 \
    --group_reporting \
    --name=$2_$3_$4_fio \
    --filesize=8G \
    --runtime=30m \
    --time_based=1 \
    --output=t1_$2_ncore$3_depth$4_fio.output
}

t1_test_wr=(write randwrite)
t1_core_numbers=(1 2 4)
t1_io_depth_arg=(1 4)

for t1_wr in ${t1_test_wr[@]};
do
    for t1_core_number in ${t1_core_numbers[@]};
    do
        for t1_io_depth in ${t1_io_depth_arg[@]};
        do
            call_fio_cmd $1 $t1_wr $t1_core_number $t1_io_depth
        done
    done
done


# test case 2
gen_fio_config()
{
    fio_config="
    [global] \n
    ioengine=psync \n
    direct=1 \n
    norandommap=1 \n
    randrepeat=0 \n
    directory=$1 \n
    filename=fix_fio_test_file \n
    blocksize_range=4k-1M \n
    iodepth=$2 \n
    group_reporting=1 \n
    filesize=8G \n
    runtime=30m \n
    time_based=1 \n
    [write-seq1] \n
    name=write-seq1 \n
    rw=write \n
    numjobs=$3 \n
    [write-seq2] \n
    name=write-seq2 \n
    rw=$4 \n
    numjobs=$3 \n
    "
    echo -e $fio_config >> t2_$3_$2_$4.fio
}

t2_test_wr=(write randwrite)
t2_core_numbers=(1 2)
t2_io_depth_arg=(1 4)

for t2_wr in ${t2_test_wr[@]};
do
    for t2_core_number in ${t2_core_numbers[@]};
    do
        for t2_io_depth in ${t2_io_depth_arg[@]};
        do
            gen_fio_config $1 $t2_io_depth $t2_core_number $t2_wr
            # Can't put <output> into fio config file
            fio t2_${t2_core_number}_${t2_io_depth}_${t2_wr}.fio \
            --output=t2_ncore${t2_core_number}_depth${t2_io_depth}_with_${t2_wr}_fio.output 
        done
    done
done

