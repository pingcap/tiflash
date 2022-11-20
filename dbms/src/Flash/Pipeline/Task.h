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

#pragma once

#include <Flash/Pipeline/Sink.h>
#include <Flash/Pipeline/Source.h>
#include <Flash/Pipeline/Transform.h>

namespace DB
{
class Task
{
public:
    Task(
        SourcePtr && source_,
        std::vector<TransformPtr> && transforms_,
        SinkPtr && sink_)
        : source(std::move(source_))
        , transforms(std::move(transforms_))
        , sink(std::move(sink_))
    {}

    std::pair<PStatus, String> execute()
    {
        try
        {
            auto [block, op_index] = fetchBlock();
            assert(!blocked_op_index);
            for (; op_index < transforms.size(); ++op_index)
            {
                auto op_status = transforms[op_index]->transform(block);
                if (op_status == PStatus::NEED_MORE)
                    continue;
                else if (op_status == PStatus::BLOCKED)
                    blocked_op_index.emplace(op_index);
                return {op_status, ""};
            }
            return {sink->write(block), ""};
        }
        catch (...)
        {
            return {PStatus::FAIL, getCurrentExceptionMessage(true, true)};
        }
    }

    bool isBlocked()
    {
        if (sink->isBlocked())
            return true;
        if (blocked_op_index && transforms[*blocked_op_index]->isBlocked())
            return true;
        if (source->isBlocked())
            return true;
        return false;
    }

private:
    // Block, next_op_index
    std::pair<Block, size_t> fetchBlock()
    {
        if (blocked_op_index)
        {
            auto op_index = *blocked_op_index;
            blocked_op_index.reset();
            return {transforms[op_index]->fetchBlock(), op_index + 1};
        }
        return {source->read(), 0};
    }

private:
    SourcePtr source;
    std::vector<TransformPtr> transforms;
    SinkPtr sink;

    std::optional<int> blocked_op_index;
};
using TaskPtr = std::unique_ptr<Task>;
} // namespace DB
