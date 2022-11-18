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

#include <Flash/Pipeline/Transform.h>
#include <Flash/Pipeline/Sink.h>
#include <Flash/Pipeline/Source.h>

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

    PStatus execute()
    {
        auto [block, op_index] = fetchBlock();
        for (; op_index < transforms.size(); ++op_index)
        {
            auto op_status = transforms[op_index]->transform(block);
            if (op_status != PStatus::NEED_MORE)
                return op_status;
        }
        return sink->write(block);
    }

    bool isBlocked()
    {
        if (sink->isBlocked())
            return true;
        for (int i = transforms.size() - 1; i >= 0; --i)
        {
            if (transforms[i]->isBlocked())
                return true;
        }
        if (source->isBlocked())
            return true;
        return false;
    }
private:
    // Block, next_op_index
    std::pair<Block, size_t> fetchBlock()
    {
        for (int i = transforms.size() - 1; i >= 0; --i)
        {
            if (auto block = transforms[i]->fetchBlock(); block)
                return {block, i + 1};
        }
        return {source->read(), 0};
    }
private:
    SourcePtr source;
    std::vector<TransformPtr> transforms;
    SinkPtr sink;
};
using TaskPtr = std::unique_ptr<Task>;
} // namespace DB
