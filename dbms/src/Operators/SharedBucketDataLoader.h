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

#include <Operators/BucketInput.h>

#include <memory>

namespace DB
{
class PipelineExecutorStatus;

class SharedBucketDataLoader : public std::enable_shared_from_this<SharedBucketDataLoader>
{
public:
    SharedBucketDataLoader(
        PipelineExecutorStatus & exec_status_,
        const BlockInputStreams & bucket_streams,
        const String & req_id,
        size_t max_queue_size_);

    // return true if pop success
    // return false means that need to continue tryPop.
    bool tryPop(BlocksList & bucket_data);

private:
    bool loadFromInputs();

    void storeFromInputToBucketData();

    void finish();

private:
    PipelineExecutorStatus & exec_status;

    LoggerPtr log;

    size_t max_queue_size;

    std::atomic_bool loading{false};

    std::atomic_bool finished{false};

    std::vector<BlocksList> bucket_data_queue;

    BucketInputs bucket_inputs;

    static constexpr Int32 NUM_BUCKETS = 256;
};
using SharedBucketDataLoaderPtr = std::shared_ptr<SharedBucketDataLoader>;

} // namespace DB
