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

#include <Transforms/Sink.h>
#include <Interpreters/AggregateStore.h>

namespace DB
{
class AggregateSink : public Sink
{
public:
    explicit AggregateSink(
        const AggregateStorePtr & aggregate_store_)
        : aggregate_store(aggregate_store_)
    {}

    bool write(Block & block, size_t loop_id) override
    {
        if (!block)
            return false;
        aggregate_store->executeOnBlock(loop_id, block);
        return block;
    }

private:
    AggregateStorePtr aggregate_store;
};
}
