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
void AggregateContext::initBuild(const Aggregator::Params & params, size_t max_threads_, Aggregator::CancellationHook && hook)
{
    assert(status.load() == AggStatus::init);
    is_cancelled = std::move(hook);
    max_threads = max_threads_;
    empty_result_for_aggregation_by_empty_set = params.empty_result_for_aggregation_by_empty_set;
    keys_size = params.keys_size;
    many_data.reserve(max_threads);
    threads_data.reserve(max_threads);
    for (size_t i = 0; i < max_threads; ++i)
    {
        threads_data.emplace_back(std::make_unique<ThreadData>(params.keys_size, params.aggregates_size));
        many_data.emplace_back(std::make_shared<AggregatedDataVariants>());
    }

    aggregator = std::make_unique<Aggregator>(params, log->identifier());
    aggregator->setCancellationHook(is_cancelled);
    aggregator->initThresholdByAggregatedDataVariantsSize(many_data.size());
    status = AggStatus::build;
    build_watch.emplace();
    LOG_TRACE(log, "Aggregate Context inited");
}

void AggregateContext::buildOnBlock(size_t task_index, const Block & block)
{
    assert(status.load() == AggStatus::build);
    aggregator->executeOnBlock(block, *many_data[task_index], threads_data[task_index]->key_columns, threads_data[task_index]->aggregate_columns);
    threads_data[task_index]->src_bytes += block.bytes();
    threads_data[task_index]->src_rows += block.rows();
}

bool AggregateContext::hasSpilledData() const
{
    assert(status.load() == AggStatus::build);
    return aggregator->hasSpilledData();
}

bool AggregateContext::needSpill(size_t task_index, bool try_mark_need_spill)
{
    assert(status.load() == AggStatus::build);
    auto & data = *many_data[task_index];
    if (try_mark_need_spill && !data.need_spill)
        data.tryMarkNeedSpill();
    return data.need_spill;
}

void AggregateContext::spillData(size_t task_index)
{
    assert(status.load() == AggStatus::build);
    aggregator->spill(*many_data[task_index]);
}

LocalAggregateRestorerPtr AggregateContext::buildLocalRestorer()
{
    assert(status.load() == AggStatus::build);
    aggregator->finishSpill();
    LOG_INFO(log, "Begin restore data from disk for local aggregation.");
    auto input_streams = aggregator->restoreSpilledData();
    status = AggStatus::restore;
    RUNTIME_CHECK_MSG(!input_streams.empty(), "There will be at least one spilled file.");
    return std::make_unique<LocalAggregateRestorer>(input_streams, *aggregator, is_cancelled, log->identifier());
}

void AggregateContext::initConvergentPrefix()
{
    assert(build_watch);
    double elapsed_seconds = build_watch->elapsedSeconds();
    size_t total_src_rows = 0;
    size_t total_src_bytes = 0;
    for (size_t i = 0; i < max_threads; ++i)
    {
        size_t rows = many_data[i]->size();
        LOG_TRACE(
            log,
            "Aggregated. {} to {} rows (from {:.3f} MiB) in {:.3f} sec. ({:.3f} rows/sec., {:.3f} MiB/sec.)",
            threads_data[i]->src_rows,
            rows,
            (threads_data[i]->src_bytes / 1048576.0),
            elapsed_seconds,
            threads_data[i]->src_rows / elapsed_seconds,
            threads_data[i]->src_bytes / elapsed_seconds / 1048576.0);
        total_src_rows += threads_data[i]->src_rows;
        total_src_bytes += threads_data[i]->src_bytes;
    }

    LOG_TRACE(
        log,
        "Total aggregated {} rows (from {:.3f} MiB) in {:.3f} sec. ({:.3f} rows/sec., {:.3f} MiB/sec.)",
        total_src_rows,
        (total_src_bytes / 1048576.0),
        elapsed_seconds,
        total_src_rows / elapsed_seconds,
        total_src_bytes / elapsed_seconds / 1048576.0);

    if (total_src_rows == 0 && keys_size == 0 && !empty_result_for_aggregation_by_empty_set)
        aggregator->executeOnBlock(
            this->getHeader(),
            *many_data[0],
            threads_data[0]->key_columns,
            threads_data[0]->aggregate_columns);
}

void AggregateContext::initConvergent()
{
    assert(status.load() == AggStatus::build);

    initConvergentPrefix();

    merging_buckets = aggregator->mergeAndConvertToBlocks(many_data, true, max_threads);
    status = AggStatus::convergent;
    RUNTIME_CHECK(!merging_buckets || merging_buckets->getConcurrency() > 0);
}

size_t AggregateContext::getConvergentConcurrency()
{
    assert(status.load() == AggStatus::convergent);
    return merging_buckets ? merging_buckets->getConcurrency() : 1;
}

Block AggregateContext::getHeader() const
{
    assert(aggregator);
    return aggregator->getHeader(true);
}

Block AggregateContext::readForConvergent(size_t index)
{
    assert(status.load() == AggStatus::convergent);
    if unlikely (!merging_buckets)
        return {};
    return merging_buckets->getData(index);
}
} // namespace DB
