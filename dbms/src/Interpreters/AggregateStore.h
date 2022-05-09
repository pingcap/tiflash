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

#include <Interpreters/Aggregator.h>

#include <memory>

namespace DB
{
using AggregateColumns = std::vector<ColumnRawPtrs>;

struct AggregateStore
{
    AggregateStore(
        const String & req_id,
        const Aggregator::Params & params,
        size_t max_threads,
        const FileProviderPtr & file_provider_,
        bool is_final_)
        : aggregator(params, req_id)
        , file_provider(file_provider_)
        , is_final(is_final_)
    {
        many_data.resize(max_threads);
        for (auto & elem : many_data)
            elem = std::make_shared<AggregatedDataVariants>();
    }

    Aggregator aggregator;
    ManyAggregatedDataVariants many_data;

    FileProviderPtr file_provider;

    bool is_final;

    size_t maxThreads() const { return many_data.size(); }

    Block getHeader() const { return aggregator.getHeader(is_final); }

    const Aggregator::Params & getParams() const { return aggregator.getParams(); }

    const AggregatedDataVariantsPtr & getData(size_t index) const { return many_data[index] }

    void executeOnBlock(
        size_t stream_index,
        const Block & block,
        ColumnRawPtrs & key_columns,
        AggregateColumns & aggregate_columns, /// Passed to not create them anew for each block
        Int64 & local_delta_memory,
        bool & no_more_keys)
    {
        aggregator.executeOnBlock(
            block,
            *many_data[stream_index],
            file_provider,
            key_columns,
            aggregate_columns,
            local_delta_memory,
            no_more_keys);
    }
};

using AggregateStorePtr = std::shared_ptr<AggregateStore>;
} // namespace DB
