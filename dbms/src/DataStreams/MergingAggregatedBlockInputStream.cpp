// Copyright 2023 PingCAP, Inc.
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

#include <Columns/ColumnsNumber.h>

#include <DataStreams/MergingAggregatedBlockInputStream.h>


namespace DB
{

Block MergingAggregatedBlockInputStream::getHeader() const
{
    return aggregator.getHeader(final);
}


Block MergingAggregatedBlockInputStream::readImpl()
{
    if (!executed)
    {
        executed = true;
        AggregatedDataVariants data_variants;

        Aggregator::CancellationHook hook = [&]() { return this->isCancelled(); };
        aggregator.setCancellationHook(hook);

        aggregator.mergeStream(children.back(), data_variants, max_threads);
        blocks = aggregator.convertToBlocks(data_variants, final, max_threads);
        it = blocks.begin();
    }

    Block res;
    if (isCancelledOrThrowIfKilled() || it == blocks.end())
        return res;

    res = std::move(*it);
    ++it;

    return res;
}


}
