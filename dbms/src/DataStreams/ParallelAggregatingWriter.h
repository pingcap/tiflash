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

#include <DataStreams/ParallelBlockInputStream.h>
#include <Interpreters/AggregateStore.h>

namespace DB
{
class ParallelAggregatingWriter : public ParallelWriter
{
public:
    static constexpr auto name = "ParallelAggregatingWriter";

    ParallelAggregatingWriter(
        const AggregateStorePtr & aggregate_store_,
        const String & req_id)
        : log(Logger::get(name, req_id))
        , aggregate_store(aggregate_store_)
    {}

    void onBlock(Block & block, size_t thread_num) override
    {
        aggregate_store->executeOnBlock(thread_num, block);
    }

    void onFinishThread(size_t thread_num) override
    {
        aggregate_store->tryFlush(thread_num);

        LOG_FMT_TRACE(
            log,
            "Aggregated. {} to {} rows (from {:.3f} MiB)).",
            aggregate_store->getThreadData(thread_num).src_rows,
            aggregate_store->getData(thread_num)->size(),
            (aggregate_store->getThreadData(thread_num).src_bytes / 1048576.0));
    }

    void onFinish() override
    {
        aggregate_store->tryFlush();
    }

private:
    const LoggerPtr log;
    AggregateStorePtr aggregate_store;
};
} // namespace DB
