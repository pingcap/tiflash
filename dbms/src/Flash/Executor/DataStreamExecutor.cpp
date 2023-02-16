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
#include <DataStreams/BlockIO.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Executor/DataStreamExecutor.h>

namespace DB
{
DataStreamExecutor::DataStreamExecutor(const BlockIO & block_io)
    : QueryExecutor(block_io.process_list_entry)
    , data_stream(block_io.in)
{
    assert(data_stream);
}

ExecutionResult DataStreamExecutor::execute(ResultHandler && result_handler)
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
    return data_stream->estimateNewThreadCount();
}

RU DataStreamExecutor::collectRequestUnit()
{
    return toRU(data_stream->estimateCPUTimeNs());
}

Block DataStreamExecutor::getSampleBlock() const
{
    return data_stream->getHeader();
}

BaseRuntimeStatistics DataStreamExecutor::getRuntimeStatistics(DAGContext & dag_context) const
{
    BaseRuntimeStatistics runtime_statistics;
    auto root_executor_id = dag_context.getRootExecutorId();
    const auto & profile_streams_map = dag_context.getProfileStreamsMap();
    auto it = profile_streams_map.find(root_executor_id);
    if (it != profile_streams_map.end())
    {
        for (const auto & input_stream : it->second)
        {
            if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(input_stream.get()); p_stream)
                runtime_statistics.append(p_stream->getProfileInfo());
        }
    }
    return runtime_statistics;
}
} // namespace DB
