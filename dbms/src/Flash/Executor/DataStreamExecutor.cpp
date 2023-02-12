// Copyright 2023 PingCAP, Ltd.
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
#include <DataStreams/BlockIO.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Executor/DataStreamExecutor.h>

namespace DB
{
DataStreamExecutor::DataStreamExecutor(const BlockIO & block_io)
    : QueryExecutor(block_io.process_list_entry)
    , data_stream(block_io.in)
{
    assert(data_stream);
    thread_cnt_before_execute = GET_METRIC(tiflash_thread_count, type_active_threads_of_thdpool).Value();
    estimate_thread_cnt = data_stream->estimateNewThreadCount();
}

ExecutionResult DataStreamExecutor::execute(ResultHandler result_handler)
{
    try
    {
        data_stream->readPrefix();
        if (result_handler.isIgnored())
        {
            while (data_stream->read())
                continue;
        }
        else
        {
            while (Block block = data_stream->read())
                result_handler(block);
        }
        data_stream->readSuffix();
        return ExecutionResult::success();
    }
    catch (...)
    {
        return ExecutionResult::fail(getCurrentExceptionMessage(true, true));
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

RU DataStreamExecutor::collectRequestUnit()
{
    auto origin_cpu_time_ns = data_stream->estimateCPUTimeNs();
    UInt64 total_thread_cnt = std::max(1, (thread_cnt_before_execute + estimate_thread_cnt));
    size_t physical_cpu_cores = getNumberOfPhysicalCPUCores();
    if (origin_cpu_time_ns <= 0 || total_thread_cnt <= physical_cpu_cores)
        return toRU(origin_cpu_time_ns);

    UInt64 per_thread_cpu_time_ns = ceil(static_cast<double>(origin_cpu_time_ns) / total_thread_cnt);
    auto cpu_time_ns = per_thread_cpu_time_ns * physical_cpu_cores;
    return toRU(cpu_time_ns);
}
} // namespace DB
