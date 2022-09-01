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
#include <Flash/Executor/DataStreamExecutor.h>

namespace DB
{
std::pair<bool, String> DataStreamExecutor::execute(ResultHandler result_handler)
{
    try
    {
        data_stream->readPrefix();
        if (result_handler.isDefault())
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
        data_stream = nullptr;
        return {true, ""};
    }
    catch (...)
    {
        return {false, getCurrentExceptionMessage(true, true)};
    }
}

BlockInputStreamPtr DataStreamExecutor::dataStream() const
{
    return data_stream;
}

String DataStreamExecutor::dump() const
{
    assert(data_stream);
    FmtBuffer fb;
    data_stream->dumpTree(fb);
    return fb.toString();
}
} // namespace DB
