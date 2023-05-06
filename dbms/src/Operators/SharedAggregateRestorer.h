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

#include <Operators/SpilledBucketInput.h>

#include <atomic>
#include <memory>
#include <queue>

namespace DB
{
class PipelineExecutorStatus;

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
class SharedSpilledBucketDataLoader : public std::enable_shared_from_this<SharedSpilledBucketDataLoader>
{
public:
    SharedSpilledBucketDataLoader(
        PipelineExecutorStatus & exec_status_,
        const BlockInputStreams & bucket_streams,
        const String & req_id,
        size_t max_queue_size_);

    ~SharedSpilledBucketDataLoader();

    // return true if pop success
    // return false means that need to continue tryPop.
    bool tryPop(BlocksList & bucket_data);

    std::vector<SpilledBucketInput *> getNeedLoadInputs();

    void storeBucketData();

private:
    void loadBucket();

    bool switchStatus(SharedLoaderStatus from, SharedLoaderStatus to);

private:
    PipelineExecutorStatus & exec_status;

    LoggerPtr log;

    size_t max_queue_size;
    std::mutex queue_mu;
    std::queue<BlocksList> bucket_data_queue;

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
    RETRY,
    FINISHED,
};

class SharedAggregateRestorer
{
public:
    SharedAggregateRestorer(
        Aggregator & aggregator_,
        SharedSpilledBucketDataLoaderPtr loader_);

    bool tryPop(Block & block);

    SharedLoadResult tryLoadBucketData();

private:
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
