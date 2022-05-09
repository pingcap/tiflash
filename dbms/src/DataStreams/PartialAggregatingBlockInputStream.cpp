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

#include <DataStreams/PartialAggregatingBlockInputStream.h>

namespace DB
{
PartialAggregatingBlockInputStream::PartialAggregatingBlockInputStream(
    const BlockInputStreamPtr & input,
    size_t stream_index_,
    const AggregateStorePtr & aggregate_store_,
    const String & req_id)
    : log(Logger::get(NAME, req_id))
    , stream_index(stream_index_)
    , aggregate_store(aggregate_store_)
{
    children.push_back(input);
}

Block PartialAggregatingBlockInputStream::readImpl()
{
    Block block = children.back()->read();
    if (block)
    {
        aggregate_store->aggregator.executeOnBlock(
            block,
            *aggregate_store->many_data[stream_index],
            aggregate_store->file_provider,
            key_columns,
            aggregate_columns,
            local_delta_memory,
            no_more_keys);
    }
    return block;
}
} // namespace DB
