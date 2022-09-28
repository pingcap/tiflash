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

#include <Core/Block.h>
#include <Interpreters/AggregateStore.h>
#include <Transforms/TryLock.h>

#include <memory>

namespace DB
{
class FinalAggregateReader
{
public:
    FinalAggregateReader(
        const AggregateStorePtr & agg_store_)
        : agg_store(agg_store_)
        , impl(agg_store->merge()) // don't need to call readPrefix/readSuffix for impl.
    {}

    Block getHeader()
    {
        assert(impl);
        return impl->getHeader();
    }

    std::pair<bool, Block> tryRead()
    {
        assert(impl);
        TryLock lock(mu);
        if (lock.isLocked())
            return {true, impl->read()};
        else
            return {false, {}};
    }

    Block read()
    {
        assert(impl);
        std::lock_guard<std::mutex> lock(mu);
        return impl->read();
    }

private:
    AggregateStorePtr agg_store;
    std::unique_ptr<IBlockInputStream> impl;
    std::mutex mu;
};

using FinalAggregateReaderPtr = std::shared_ptr<FinalAggregateReader>;
} // namespace DB
