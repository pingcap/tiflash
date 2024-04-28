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

#include <Operators/AggregateContext.h>

namespace DB
{
void AggregateContext::initBuild(
    const Aggregator::Params & params,
    size_t max_threads_,
    Aggregator::CancellationHook && hook,
    const RegisterOperatorSpillContext & register_operator_spill_context)
{
    assert(status.load() == AggStatus::init);
    is_cancelled = std::move(hook);
    max_threads = max_threads_;
    empty_result_for_aggregation_by_empty_set = params.empty_result_for_aggregation_by_empty_set;
    keys_size = params.keys_size;
    aggregator = std::make_unique<Aggregator>(params, log->identifier(), max_threads, register_operator_spill_context);
    aggregator->setCancellationHook(is_cancelled);
    aggregator->initThresholdByAggregatedDataVariantsSize(max_threads);
    many_data.reserve(max_threads);
    threads_data.reserve(max_threads);
    for (size_t i = 0; i < max_threads; ++i)
    {
        threads_data.emplace_back(std::make_unique<ThreadData>(aggregator.get()));
        many_data.emplace_back(std::make_shared<AggregatedDataVariants>());
    }
    status = AggStatus::build;
    build_watch.emplace();
    LOG_TRACE(log, "Aggregate Context inited");
}

void AggregateContext::buildOnLocalData(size_t task_index)
{
    auto & agg_process_info = threads_data[task_index]->agg_process_info;
    aggregator->executeOnBlock(agg_process_info, *many_data[task_index], task_index);
    if likely (agg_process_info.allBlockDataHandled())
    {
        threads_data[task_index]->src_bytes += agg_process_info.block.bytes();
        threads_data[task_index]->src_rows += agg_process_info.block.rows();
        agg_process_info.block.clear();
    }
}

bool AggregateContext::isTaskMarkedForSpill(size_t task_index)
{
    if (needSpill(task_index))
        return true;
    if (getAggSpillContext()->updatePerThreadRevocableMemory(many_data[task_index]->revocableBytes(), task_index))
    {
        assert(!many_data[task_index]->empty());
        return many_data[task_index]->tryMarkNeedSpill();
    }
    return false;
}

bool AggregateContext::hasLocalDataToBuild(size_t task_index)
{
    return !threads_data[task_index]->agg_process_info.allBlockDataHandled();
}

void AggregateContext::buildOnBlock(size_t task_index, const Block & block)
{
    assert(status.load() == AggStatus::build);
    auto & agg_process_info = threads_data[task_index]->agg_process_info;
    agg_process_info.resetBlock(block);
    buildOnLocalData(task_index);
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
    aggregator->spill(*many_data[task_index], task_index);
}

LocalAggregateRestorerPtr AggregateContext::buildLocalRestorer()
{
    assert(status.load() == AggStatus::build);
    aggregator->finishSpill();
    LOG_INFO(log, "Begin restore data from disk for local aggregation.");
    auto input_streams = aggregator->restoreSpilledData();
    RUNTIME_CHECK_MSG(!input_streams.empty(), "There will be at least one spilled file.");
    status = AggStatus::restore;
    return std::make_unique<LocalAggregateRestorer>(input_streams, *aggregator, is_cancelled, log->identifier());
}

std::vector<SharedAggregateRestorerPtr> AggregateContext::buildSharedRestorer(PipelineExecutorContext & exec_context)
{
    assert(status.load() == AggStatus::build);
    aggregator->finishSpill();
    LOG_INFO(log, "Begin restore data from disk for shared aggregation.");
    auto input_streams = aggregator->restoreSpilledData();
    RUNTIME_CHECK_MSG(!input_streams.empty(), "There will be at least one spilled file.");
    auto loader
        = std::make_shared<SharedSpilledBucketDataLoader>(exec_context, input_streams, log->identifier(), max_threads);
    std::vector<SharedAggregateRestorerPtr> ret;
    for (size_t i = 0; i < max_threads; ++i)
        ret.push_back(std::make_unique<SharedAggregateRestorer>(*aggregator, loader));
    status = AggStatus::restore;
    return ret;
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
    {
        auto & agg_process_info = threads_data[0]->agg_process_info;
        agg_process_info.resetBlock(this->getSourceHeader());
        aggregator->executeOnBlock(agg_process_info, *many_data[0], 0);
        /// Since this won't consume a lot of memory,
        /// even if it triggers marking need spill due to a low threshold setting,
        /// it's still reasonable not to spill disk.
        many_data[0]->need_spill = false;
        assert(agg_process_info.allBlockDataHandled());
        RUNTIME_CHECK(!aggregator->hasSpilledData());
    }
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

Block AggregateContext::getSourceHeader() const
{
    assert(aggregator);
    return aggregator->getSourceHeader();
}

Block AggregateContext::readForConvergent(size_t index)
{
    assert(status.load() == AggStatus::convergent);
    if unlikely (!merging_buckets)
        return {};

    return merging_buckets->getData(index, /*enable_skip_serialize_key=*/true);
}

bool AggregateContext::hasAtLeastOneTwoLevel()
{
    for (size_t i = 0; i < max_threads; ++i)
    {
        if (many_data[i]->isTwoLevel())
            return true;
    }
    return false;
}

} // namespace DB
