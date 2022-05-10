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
    size_t stream_index,
    const AggregateStorePtr & aggregate_store_,
    const String & req_id)
    : log(Logger::get(NAME, req_id))
    , thread_index(stream_index % aggregate_store_->max_streams)
    , aggregate_store(aggregate_store_)
{
    children.push_back(input);
}

Block PartialAggregatingBlockInputStream::readImpl()
{
    Block block = children.back()->read();
    if (block)
    {
        aggregate_store->executeOnBlock(thread_index, block);
        return block;
    }
    else
    {
        double elapsed_seconds = static_cast<double>(getProfileInfo().execution_time) / 1000000;
        LOG_FMT_TRACE(
            log,
            "Aggregated. {} to {} rows (from {:.3f} MiB) in {:.3f} sec. ({:.3f} rows/sec., {:.3f} MiB/sec.)",
            getProfileInfo().rows,
            aggregate_store->getData(stream_index)->size(),
            (getProfileInfo().bytes / 1048576.0),
            elapsed_seconds,
            getProfileInfo().rows / elapsed_seconds,
            getProfileInfo().bytes / elapsed_seconds / 1048576.0);

        if (!isCancelled())
        {
            aggregate_store->tryFlush(thread_index);
        }
        return {};
    }
}
} // namespace DB
