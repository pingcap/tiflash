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

#include <Interpreters/AggregateStore.h>
#include <Transforms/Sink.h>

namespace DB
{
class AggregateSink : public Sink
{
public:
    AggregateSink(
        const AggregateStorePtr & aggregate_store_,
        size_t index_)
        : aggregate_store(aggregate_store_)
        , index(index_)
    {}

    bool write(Block & block) override
    {
        if (unlikely(!block))
            return false;
        aggregate_store->executeOnBlock(index, block);
        return true;
    }

private:
    AggregateStorePtr aggregate_store;
    size_t index;
};
} // namespace DB
