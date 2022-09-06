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

#include <Common/Exception.h>
#include <Core/Block.h>
#include <Transforms/Source.h>
#include <Transforms/Sink.h>

#include <atomic>
#include <memory>

namespace DB
{
namespace ErrorCodes
{
extern const int QUERY_WAS_CANCELLED;
}

class Transform
{
public:
    virtual ~Transform() = default;

    virtual bool transform(Block & block) = 0;
};
using TransformPtr = std::shared_ptr<Transform>;

class Transforms
{
public:
    void set(const SourcePtr & source_)
    {
        assert(!source);
        source = source_;
    }
    void set(const SinkPtr & sink_)
    {
        assert(!sink);
        sink = sink_;
    }
    void append(const TransformPtr & transform)
    {
        transforms.emplace_back(transform);
    }

    bool execute(size_t loop_id)
    {
        assert(source);
        assert(sink);

        if (isCancelledOrThrowIfKilled())
            return false;

        Block block = source->read();
        for (const auto & transform : transforms)
        {
            if (!transform->transform(block))
                return true;
        }
        return sink->write(block, loop_id);
    }

    void prepare() {}
    void finish() {}

    void cancel(bool kill)
    {
        if (kill)
            is_killed = true;
        is_cancelled = true;
    }

    bool isCancelledOrThrowIfKilled() const
    {
        if (!is_cancelled)
            return false;
        if (is_killed)
            throw Exception("Query was cancelled", ErrorCodes::QUERY_WAS_CANCELLED);
        return true;
    }

private:
    SourcePtr source;
    std::vector<TransformPtr> transforms;
    SinkPtr sink;

    std::atomic<bool> is_cancelled{false};
    std::atomic<bool> is_killed{false};
};
using TransformsPtr = std::shared_ptr<Transforms>;
}
