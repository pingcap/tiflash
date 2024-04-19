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

#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Interpreters/Aggregator.h>
#include <Operators/LocalAggregateRestorer.h>
#include <Operators/SharedAggregateRestorer.h>

namespace DB
{
struct ThreadData
{
    size_t src_rows = 0;
    size_t src_bytes = 0;

    Aggregator::AggProcessInfo agg_process_info;
    explicit ThreadData(Aggregator * aggregator)
        : agg_process_info(aggregator)
    {}
};

/// Aggregated data shared between AggBuild and AggConvergent Pipeline.
class AggregateContext
{
public:
    explicit AggregateContext(const String & req_id)
        : log(Logger::get(req_id))
    {}

    void initBuild(
        const Aggregator::Params & params,
        size_t max_threads_,
        Aggregator::CancellationHook && hook,
        const RegisterOperatorSpillContext & register_operator_spill_context);

    size_t getBuildConcurrency() const { return max_threads; }

    void buildOnBlock(size_t task_index, const Block & block);

    bool hasSpilledData() const;

    bool needSpill(size_t task_index, bool try_mark_need_spill = false);

    void spillData(size_t task_index);

    LocalAggregateRestorerPtr buildLocalRestorer();

    std::vector<SharedAggregateRestorerPtr> buildSharedRestorer(PipelineExecutorContext & exec_context);

    void initConvergent();

    // Called before convergent to trace aggregate statistics and handle empty table with result case.
    void initConvergentPrefix();

    size_t getConvergentConcurrency();

    Block readForConvergent(size_t index);

    Block getHeader() const;

    Block getSourceHeader() const;

    AggSpillContextPtr & getAggSpillContext() { return aggregator->getAggSpillContext(); }

    bool hasLocalDataToBuild(size_t task_index);

    void buildOnLocalData(size_t task_index);

    bool isTaskMarkedForSpill(size_t task_index);

    size_t getTotalBuildRows(size_t task_index) { return threads_data[task_index]->src_rows; }

    bool hasAtLeastOneTwoLevel();
    bool isConvertibleToTwoLevel() const { return aggregator->isConvertibleToTwoLevel(); }
    bool isTwoLevelOrEmpty(size_t task_index) const
    {
        return many_data[task_index]->isTwoLevel() || many_data[task_index]->empty();
    }
    void convertToTwoLevel(size_t task_index) { many_data[task_index]->convertToTwoLevel(); }

private:
    std::unique_ptr<Aggregator> aggregator;
    bool keys_size = false;
    bool empty_result_for_aggregation_by_empty_set = false;

    /**
     * init────►build───┬───►convergent
     *                  │
     *                  ▼
     *               restore
     */
    enum class AggStatus
    {
        init,
        build,
        convergent,
        restore,
    };
    std::atomic<AggStatus> status{AggStatus::init};

    Aggregator::CancellationHook is_cancelled{[]() {
        return false;
    }};

    MergingBucketsPtr merging_buckets;
    ManyAggregatedDataVariants many_data;
    // use unique_ptr to avoid false sharing.
    std::vector<std::unique_ptr<ThreadData>> threads_data;
    size_t max_threads{};

    const LoggerPtr log;

    std::optional<Stopwatch> build_watch;
};

using AggregateContextPtr = std::shared_ptr<AggregateContext>;
} // namespace DB
