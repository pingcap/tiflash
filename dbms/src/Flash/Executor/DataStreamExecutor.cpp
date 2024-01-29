// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/FmtUtils.h>
#include <Common/TiFlashMetrics.h>
#include <Common/getNumberOfCPUCores.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Executor/DataStreamExecutor.h>

namespace DB
{
DataStreamExecutor::DataStreamExecutor(
    const MemoryTrackerPtr & memory_tracker_,
    Context & context_,
    const String & req_id,
    const BlockInputStreamPtr & data_stream_)
    : QueryExecutor(memory_tracker_, context_, req_id)
    , data_stream(data_stream_)
{
    assert(data_stream);
    thread_cnt_before_execute = GET_METRIC(tiflash_thread_count, type_active_threads_of_thdpool).Value();
    estimate_thread_cnt = std::max(data_stream->estimateNewThreadCount(), 1);
}

ExecutionResult DataStreamExecutor::execute(ResultHandler && result_handler)
{
    try
    {
        data_stream->readPrefix();
        if (result_handler)
        {
            while (Block block = data_stream->read())
                result_handler(block);
        }
        else
        {
            while (data_stream->read())
                continue;
        }
        data_stream->readSuffix();
        return ExecutionResult::success();
    }
    catch (...)
    {
        return ExecutionResult::fail(std::current_exception());
    }
}

void DataStreamExecutor::cancel()
{
    if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(data_stream.get()); p_stream)
        p_stream->cancel(/*kill=*/false);
}

String DataStreamExecutor::toString() const
{
    FmtBuffer fb;
    data_stream->dumpTree(fb);
    return fb.toString();
}

int DataStreamExecutor::estimateNewThreadCount()
{
    return estimate_thread_cnt;
}

// TODO fix the data race on BlockStreamProfileInfo when the stream throws an error
// and remove `race:dbms/src/DataStreams/BlockStreamProfileInfo.h` in tests/sanitize/tsan.suppression.
// https://github.com/pingcap/tiflash/issues/7631
UInt64 DataStreamExecutor::collectCPUTimeNs()
{
    // The cputime returned by BlockInputSrream is a count of the execution time of each thread.
    // However, cputime here is imprecise and is affected by thread scheduling and condition_cv.wait.
    // The following attempts to eliminate the effects of thread scheduling.

    auto execute_time_ns = data_stream->estimateCPUTimeNs();
    UInt64 total_thread_cnt = thread_cnt_before_execute + estimate_thread_cnt;
    size_t logical_cpu_cores = getNumberOfLogicalCPUCores();
    // When the number of threads is greater than the number of cpu cores,
    // BlockInputStream's estimated cpu time will be much greater than the actual value.
    if (execute_time_ns <= 0 || total_thread_cnt <= logical_cpu_cores)
        return execute_time_ns;

    // Here we use `execute_time_ns / thread_cnt` to get the average execute time of each thread.
    // So we have `per_thread_execute_time_ns = execute_time_ns / estimate_thread_cnt`.
    // Since a cpu core executes several threads, think of `execute_time_of_thread_A = cputime_of_thread_A + cputime_of_other_thread`.
    // Assuming that the cputime of other threads is the same as thread A, then we have `execute_time_of_thread_A = total_thread_cnt * cputime_of_thread_A`.
    // Assuming that threads is divided equally among all cpu cores, then `the number of threads allocated to cpu core A = total_thread_cnt / logical_cpu_cores`.
    // So we have `per_thread_cputime_ns = per_thread_execute_time_ns / (total_thread_cnt / logical_cpu_cores)`.
    // And the number of threads of `data_stream` executed by one cpu core is `estimate_thread_cnt / logical_cpu_cores`.
    // So we have `per_core_cputime_ns = per_thread_cputime_ns * (estimate_thread_cnt / logical_cpu_cores)`
    // So it can be assumed that
    //     per_core_cpu_time = per_thread_cputime_ns * (estimate_thread_cnt / logical_cpu_cores)
    //                       = per_thread_execute_time_ns / (total_thread_cnt / logical_cpu_cores) * (estimate_thread_cnt / logical_cpu_cores)
    //                       = per_thread_execute_time_ns / total_thread_cnt * estimate_thread_cnt
    // So that we have
    //     cputime_ns = per_core_cpu_time * logical_cpu_cores
    //                = per_thread_execute_time_ns / total_thread_cnt * estimate_thread_cnt * logical_cpu_cores
    //                = execute_time_ns / total_thread_cnt / estimate_thread_cnt * estimate_thread_cnt * logical_cpu_cores
    //                = execute_time_ns / total_thread_cnt * logical_cpu_cores
    auto cpu_time_ns = static_cast<double>(execute_time_ns) / total_thread_cnt * logical_cpu_cores;
    // But there is still no way to eliminate the effect of `condition_cv.wait` here...
    // We can assume `condition.wait` takes half of datastream execute time.
    // TODO find a more reasonable ratio for `condition.wait`.
    cpu_time_ns /= 2;
    return static_cast<UInt64>(ceil(cpu_time_ns));
}

Block DataStreamExecutor::getSampleBlock() const
{
    return data_stream->getHeader();
}

BaseRuntimeStatistics DataStreamExecutor::getRuntimeStatistics() const
{
    BaseRuntimeStatistics runtime_statistics;
    assert(data_stream);
    if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(data_stream.get()))
        runtime_statistics.append(p_stream->getProfileInfo());
    return runtime_statistics;
}
} // namespace DB
