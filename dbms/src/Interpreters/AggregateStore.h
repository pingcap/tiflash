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

#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>
#include <DataStreams/TemporaryFileStream.h>
#include <Interpreters/Aggregator.h>

#include <memory>
#include <utility>

namespace ProfileEvents
{
extern const Event ExternalAggregationMerge;
}

namespace DB
{
struct ThreadData
{
    size_t src_rows = 0;
    size_t src_bytes = 0;

    Int64 local_delta_memory = 0;

    ColumnRawPtrs key_columns;
    /// Passed to not create them anew for each block
    Aggregator::AggregateColumns aggregate_columns;

    /** Used if there is a limit on the maximum number of rows in the aggregation,
      *  and if group_by_overflow_mode == ANY.
      * In this case, new keys are not added to the set, but aggregation is performed only by
      *  keys that have already been added into the set.
      */
    bool no_more_keys = false;

    ThreadData(
        size_t keys_size,
        size_t aggregates_size)
    {
        key_columns.resize(keys_size);
        aggregate_columns.resize(aggregates_size);
    }
};

class AggregateStore
{
public:
    AggregateStore(
        const String & req_id,
        const Aggregator::Params & params,
        const FileProviderPtr & file_provider_,
        bool is_final_,
        size_t max_threads_,
        size_t temporary_data_merge_threads_);

    Block getHeader() const;

    const Aggregator::Params & getParams() const;

    const AggregatedDataVariantsPtr & getData(size_t index) const;

    void executeOnBlock(size_t index, const Block & block);

    void tryFlush(size_t index);

    void tryFlush();

    std::unique_ptr<IBlockInputStream> merge();

    // total_src_rows, total_src_bytes
    std::pair<size_t, size_t> mergeSrcRowsAndBytes() const;

public:
    const FileProviderPtr file_provider;

    const bool is_final;

    const size_t max_threads;
    const size_t temporary_data_merge_threads;

private:
    const LoggerPtr log;

    Aggregator aggregator;

    std::vector<ThreadData> threads_data;
    ManyAggregatedDataVariants many_data;

    std::vector<std::unique_ptr<TemporaryFileStream>> temporary_inputs;
};

using AggregateStorePtr = std::shared_ptr<AggregateStore>;
} // namespace DB
