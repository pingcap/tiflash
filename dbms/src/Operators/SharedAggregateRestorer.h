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

#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
#include <Flash/Pipeline/Schedule/Tasks/PipeConditionVariable.h>
#include <Operators/SpilledBucketInput.h>

#include <atomic>
#include <memory>
#include <queue>

namespace DB
{
class PipelineExecutorContext;

class Aggregator;

enum class SharedLoaderStatus
{
    idle,
    loading,
    finished,
};

/**
 * ┌──────────────────────────────────────────────────┐
 * │  {bucket0, bucket1, ... bucket255}spilled_file0──┼─►LoadBucketTask1───┐
 * │  {bucket0, bucket1, ... bucket255}spilled_file1──┼─►LoadBucketTask2───┤
 * │  {bucket0, bucket1, ... bucket255}spilled_file2──┼─►LoadBucketTask3───┤
 * │  ...                                             │  ...               │
 * │  {bucket0, bucket1, ... bucket255}spilled_filen──┼─►LoadBucketTaskn───┤
 * └──────────────────────────────────────────────────┘                    │
 *                                                                         │
 *               LoadBucketEvent◄──────────────────────────────────────────┘
 *                      ▲ 
 *                      │ 1 concurrency loop
 *                      ▼
 *             SharedSpilledBucketDataLoader
 *                      │
 *                      │    ┌─────►SharedAggregateRestorer1
 *                      │    ├─────►SharedAggregateRestorer2
 *                      └────┼─────►SharedAggregateRestorer3
 *                           ...
 *                           └─────►SharedAggregateRestorern
 */
class SharedSpilledBucketDataLoader
    : public std::enable_shared_from_this<SharedSpilledBucketDataLoader>
    , public NotifyFuture
{
public:
    SharedSpilledBucketDataLoader(
        PipelineExecutorContext & exec_context_,
        const BlockInputStreams & bucket_streams,
        const String & req_id,
        size_t max_queue_size_);

    ~SharedSpilledBucketDataLoader() override;

    // return true if pop success
    // return false means that need to continue tryPop.
    bool tryPop(BlocksList & bucket_data);

    std::vector<SpilledBucketInput *> getNeedLoadInputs();

    void storeBucketData();

    void registerTask(TaskPtr && task) override;

private:
    void loadBucket();

    bool switchStatus(SharedLoaderStatus from, SharedLoaderStatus to);

    bool checkCancelled();

private:
    PipelineExecutorContext & exec_context;

    LoggerPtr log;

    size_t max_queue_size;
    std::mutex mu;
    std::queue<BlocksList> bucket_data_queue;

    bool is_cancelled{false};

    PipeConditionVariable pipe_read_cv;

    // `bucket_inputs` will only be modified in `toFinishStatus` and `storeFromInputToBucketData` and always in `SharedLoaderStatus::loading`.
    // The unique_ptr of spilled file is held by SpilledBucketInput, so don't need to care about agg_context.
    SpilledBucketInputs bucket_inputs;
    static constexpr Int32 NUM_BUCKETS = 256;

    std::atomic<SharedLoaderStatus> status{SharedLoaderStatus::idle};
};
using SharedSpilledBucketDataLoaderPtr = std::shared_ptr<SharedSpilledBucketDataLoader>;

enum class SharedLoadResult
{
    SUCCESS,
    WAIT,
    FINISHED,
};

class SharedAggregateRestorer
{
public:
    SharedAggregateRestorer(Aggregator & aggregator_, SharedSpilledBucketDataLoaderPtr loader_);

    bool tryPop(Block & block);

private:
    SharedLoadResult tryLoadBucketData();

    Block popFromRestoredBlocks();

private:
    Aggregator & aggregator;

    // loader --> bucket_data --> restored_blocks.
    BlocksList bucket_data;
    BlocksList restored_blocks;

    SharedSpilledBucketDataLoaderPtr loader;
};
using SharedAggregateRestorerPtr = std::unique_ptr<SharedAggregateRestorer>;

} // namespace DB
