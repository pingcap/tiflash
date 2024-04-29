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

#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/Aggregator.h>

namespace DB
{
template <typename Method>
class AggHashTableToBlocksBlockInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "AggHashTableToBlocks";

public:
    AggHashTableToBlocksBlockInputStream(
        Aggregator & aggregator_,
        AggregatedDataVariants & aggregated_data_variants_,
        Method & method_)
        : aggregator(aggregator_)
        , aggregated_data_variants(aggregated_data_variants_)
        , method(method_)
        , max_block_rows(0)
        , max_block_bytes(0)
        , current_bucket(0)
        , total_bucket(Method::Data::NUM_BUCKETS)
    {}

    Block getHeader() const override { return aggregator.getHeader(false); }
    String getName() const override { return NAME; }
    std::pair<size_t, size_t> maxBlockRowAndBytes() const { return {max_block_rows, max_block_bytes}; }

protected:
    Block readImpl() override
    {
        if (current_bucket < total_bucket)
        {
            auto block = aggregator.convertOneBucketToBlock(
                aggregated_data_variants,
                method,
                aggregated_data_variants.aggregates_pool,
                false,
                current_bucket++,
                /*enable_convert_key_optimization=*/false);
            size_t block_rows = block.rows();
            size_t block_bytes = block.bytes();

            if (block_rows > max_block_rows)
                max_block_rows = block_rows;
            if (block_bytes > max_block_bytes)
                max_block_bytes = block_bytes;
            return block;
        }
        return {};
    }

private:
    Aggregator & aggregator;
    AggregatedDataVariants & aggregated_data_variants;
    Method & method;
    size_t max_block_rows;
    size_t max_block_bytes;
    size_t current_bucket;
    size_t total_bucket;
};
} // namespace DB
