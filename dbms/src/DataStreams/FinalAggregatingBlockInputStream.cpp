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

#include <DataStreams/FinalAggregatingBlockInputStream.h>

namespace DB
{
FinalAggregatingBlockInputStream::FinalAggregatingBlockInputStream(
    const BlockInputStreamPtr & input,
    const AggregateStorePtr & aggregate_store_,
    const String & req_id)
    : log(Logger::get(NAME, req_id))
    , aggregate_store(aggregate_store_)
{
    children.push_back(input);
}

void FinalAggregatingBlockInputStream::prepare()
{
    if (prepared)
        return;

    Stopwatch watch;

    const auto child = children.back();
    while (child->read()) {}

    if (!isCancelled())
    {
        aggregate_store->tryFlush();
    }

    double elapsed_seconds = watch.elapsedSeconds();

    auto [total_src_rows, total_src_bytes] = aggregate_store->mergeSrcRowsAndBytes();
    LOG_FMT_TRACE(
        log,
        "Total aggregated. {} rows (from {:.3f} MiB) in {:.3f} sec. ({:.3f} rows/sec., {:.3f} MiB/sec.)",
        total_src_rows,
        (total_src_bytes / 1048576.0),
        elapsed_seconds,
        total_src_rows / elapsed_seconds,
        total_src_bytes / elapsed_seconds / 1048576.0);

    /// If there was no data, and we aggregate without keys, we must return single row with the result of empty aggregation.
    /// To do this, we pass a block with zero rows to aggregate.
    const auto & params = aggregate_store->getParams();
    if (total_src_rows == 0 && params.keys_size == 0 && !params.empty_result_for_aggregation_by_empty_set)
        aggregate_store->executeOnBlock(0, children.back()->getHeader());

    impl = aggregate_store->merge();

    prepared = true;
}

Block FinalAggregatingBlockInputStream::readImpl()
{
    prepare();

    Block res;
    if (isCancelledOrThrowIfKilled() || !impl)
        return res;

    return impl->read();
}
} // namespace DB
