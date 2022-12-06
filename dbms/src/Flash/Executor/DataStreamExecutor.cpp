// Copyright 2022 PingCAP, Ltd.
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
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Executor/DataStreamExecutor.h>
#include <thread>

namespace DB
{
ExecutionResult DataStreamExecutor::execute(ResultHandler result_handler)
{
    try
    {
        auto myid = std::this_thread::get_id();
        std::stringstream ss;
        ss << myid;
        std::string tid = ss.str();

        auto * log = &Poco::Logger::get("LRUCache");
        data_stream->readPrefix();
        if (result_handler.isIgnored())
        {
            LOG_INFO(log, "ENTRY: in {}", tid);
            while (data_stream->read())
            {
                LOG_INFO(log, "ENTRY: out {}", tid);
                LOG_INFO(log, "ENTRY: in {}", tid);
                // continue;
            }
            LOG_INFO(log, "ENTRY: out {}", tid);
        }
        else
        {
            LOG_INFO(log, "ENTRY: in {}", tid);
            while (Block block = data_stream->read())
            {
                LOG_INFO(log, "ENTRY: out {}", tid);
                LOG_INFO(log, "ENTRY: in {}", tid);
                result_handler(block);
            }
            LOG_INFO(log, "ENTRY: out {}", tid);
        }
        data_stream->readSuffix();
        LOG_INFO(log, "ENTRY: finish {}", tid);
        return ExecutionResult::success();
    }
    catch (...)
    {
        return ExecutionResult::fail(getCurrentExceptionMessage(true, true));
    }
}

void DataStreamExecutor::cancel(bool is_kill)
{
    if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(data_stream.get()); p_stream)
        p_stream->cancel(is_kill);
}

String DataStreamExecutor::dump() const
{
    FmtBuffer fb;
    data_stream->dumpTree(fb);
    return fb.toString();
}

int DataStreamExecutor::estimateNewThreadCount()
{
    return data_stream->estimateNewThreadCount();
}
} // namespace DB
