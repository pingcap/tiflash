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

#pragma once

#include <Common/Logger.h>
#include <DataStreams/MergingAndConvertingBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Interpreters/Aggregator.h>
#include <Operators/Operator.h>

#include <memory>
#include <mutex>

namespace DB
{

struct ThreadData
{
    size_t src_rows = 0;
    size_t src_bytes = 0;

    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns aggregate_columns;

    ThreadData(size_t keys_size, size_t aggregates_size)
    {
        key_columns.resize(keys_size);
        aggregate_columns.resize(aggregates_size);
    }
};

class AggregateContext
{
public:
    AggregateContext(
        bool final_,
        const String & req_id)
        : log(Logger::get(req_id))
        , is_final(final_)
    {
    }

    void init(const Aggregator::Params & params, size_t max_threads_)
    {
        max_threads = max_threads_;
        std::cout << "max_thread: " << max_threads << std::endl;
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

    void executeOnBlock(size_t task_index, const Block & block)
    {
        aggregator->executeOnBlock(block, *many_data[task_index], threads_data[task_index].key_columns, threads_data[task_index].aggregate_columns);
        threads_data[task_index].src_bytes += block.bytes();
        threads_data[task_index].src_rows += block.rows();
    }

    void initMerge()
    {
        std::unique_lock lock(mu);
        if (inited)
        {
            return;
        }

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

    void read(Block & block)
    {
        std::unique_lock lock(mu);
        RUNTIME_CHECK(inited == true);
        block = impl->read();
    }

    Block getHeader() const
    {
        return aggregator->getHeader(is_final);
    }

private:
    std::unique_ptr<Aggregator> aggregator;
    const LoggerPtr log;
    ManyAggregatedDataVariants many_data;
    std::vector<ThreadData> threads_data;
    size_t max_threads{};
    bool is_final{};
    std::unique_ptr<IBlockInputStream> impl;
    bool inited = false;
    mutable std::shared_mutex mu;
};

using AggregateContextPtr = std::shared_ptr<AggregateContext>;
} // namespace DB