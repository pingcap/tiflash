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

#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>
#include <DataStreams/MergingAndConvertingBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>

namespace DB
{
Block AggregatingBlockInputStream::getHeader() const
{
    return aggregator.getHeader(final);
}


Block AggregatingBlockInputStream::readImpl()
{
    if (!executed)
    {
        executed = true;
        AggregatedDataVariantsPtr data_variants = std::make_shared<AggregatedDataVariants>();

        CancellationHook hook = [&]() {
            return this->isCancelled();
        };
        aggregator.setCancellationHook(hook);
        aggregator.initThresholdByAggregatedDataVariantsSize(1);

        aggregator.execute(children.back(), *data_variants, 0);

        /// no new spill can be triggered anymore
        aggregator.getAggSpillContext()->finishSpillableStage();

        if (!aggregator.hasSpilledData() && !aggregator.getAggSpillContext()->isThreadMarkedForAutoSpill(0))
        {
            ManyAggregatedDataVariants many_data{data_variants};
            auto merging_buckets = aggregator.mergeAndConvertToBlocks(many_data, final, 1);
            if (!merging_buckets)
            {
                impl = std::make_unique<NullBlockInputStream>(aggregator.getHeader(final));
            }
            else
            {
                RUNTIME_CHECK(1 == merging_buckets->getConcurrency());
                impl = std::make_unique<MergingAndConvertingBlockInputStream>(merging_buckets, 0, log->identifier());
            }
        }
        else
        {
            /** If there are temporary files with partially-aggregated data on the disk,
              *  then read and merge them, spending the minimum amount of memory.
              */

            if (!isCancelled())
            {
                /// Flush data in the RAM to disk also. It's easier than merging on-disk and RAM data.
                if (data_variants->tryMarkNeedSpill())
                    aggregator.spill(*data_variants, 0);
            }
            aggregator.finishSpill();
            LOG_INFO(log, "Begin restore data from disk for aggregation.");
            BlockInputStreams input_streams = aggregator.restoreSpilledData();
            impl = std::make_unique<MergingAggregatedMemoryEfficientBlockInputStream>(
                input_streams,
                params,
                final,
                1,
                1,
                log->identifier());
        }
    }

    if (isCancelledOrThrowIfKilled() || !impl)
        return {};

    return impl->read();
}

} // namespace DB
