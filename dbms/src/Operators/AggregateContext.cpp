// Copyright 2023 PingCAP, Ltd.
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

#include <Operators/AggregateContext.h>

namespace DB
{
void AggregateContext::init(const Aggregator::Params & params, size_t max_threads_)
{
    max_threads = max_threads_;
    many_data.reserve(max_threads);
    threads_data.reserve(max_threads);
    for (size_t i = 0; i < max_threads; ++i)
    {
        threads_data.emplace_back(params.keys_size, params.aggregates_size);
        many_data.emplace_back(std::make_shared<AggregatedDataVariants>());
    }

    aggregator = std::make_unique<Aggregator>(params, log->identifier());
    aggregator->initThresholdByAggregatedDataVariantsSize(many_data.size());
    LOG_TRACE(log, "Aggregate Context inited");
}

void AggregateContext::executeOnBlock(size_t task_index, const Block & block)
{
    aggregator->executeOnBlock(block, *many_data[task_index], threads_data[task_index].key_columns, threads_data[task_index].aggregate_columns);
    threads_data[task_index].src_bytes += block.bytes();
    threads_data[task_index].src_rows += block.rows();
}

void AggregateContext::initConvergent()
{
    std::unique_lock lock(mu);
    if (inited)
        return;

    auto merging_buckets = aggregator->mergeAndConvertToBlocks(many_data, is_final, max_threads);
    if (!merging_buckets)
    {
        impl = std::make_unique<NullBlockInputStream>(aggregator->getHeader(is_final));
    }
    else
    {
        RUNTIME_CHECK(merging_buckets->getConcurrency() > 0);
        if (merging_buckets->getConcurrency() > 1)
        {
            BlockInputStreams merging_streams;
            for (size_t i = 0; i < merging_buckets->getConcurrency(); ++i)
                merging_streams.push_back(
                    std::make_shared<MergingAndConvertingBlockInputStream>(merging_buckets, i, log->identifier()));
            impl = std::make_unique<UnionBlockInputStream<>>(
                merging_streams,
                BlockInputStreams{},
                max_threads,
                log->identifier());
        }
        else
        {
            impl = std::make_unique<MergingAndConvertingBlockInputStream>(merging_buckets, 0, log->identifier());
        }
    }
    inited = true;
}

void AggregateContext::read(Block & block)
{
    std::unique_lock lock(mu);
    RUNTIME_CHECK(inited == true);
    block = impl->read();
}
} // namespace DB
