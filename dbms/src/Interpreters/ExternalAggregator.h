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

#include <Core/Block.h>
#include <Core/Spiller.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Aggregator.h>

#include <memory>
#include <random>

namespace DB
{
class ExternalAggregator
{
public:
    ExternalAggregator(
        const Aggregator::Params & params,
        const Block & header_,
        const AggregatedDataVariants::Type & type,
        const String & req_id);

    void spill(std::function<Block(size_t)> && get_bucket_block);
    void finishSpill();

    bool hasSpilledData() const;
    bool hasRestoreData() const;
    BlocksList restoreBucketBlocks();

    /** Set a function that checks whether the current task can be aborted.
      */
    void setCancellationHook(Aggregator::CancellationHook cancellation_hook);

private:
    void sortBaseSpill(std::function<Block(size_t)> && get_bucket_block, std::function<void(const Block &)> update_max_sizes);
    void partitionBaseSpill(std::function<Block(size_t)> && get_bucket_block, std::function<void(const Block &)> update_max_sizes);

    void prepareForRestore();

    BlocksList sortBaseRestore();
    BlocksList partitionBaseRestore();

private:
    bool is_local_agg;
    Block header;
    size_t bucket_num;

    LoggerPtr log;

    /// for spill
    size_t partition_num;
    SpillerPtr spiller;

    std::mt19937 gen{std::random_device{}()};

    /// for restore
    class Input
    {
    public:
        explicit Input(const BlockInputStreamPtr & stream_);

        bool hasOutput() const;
        Block moveOutput();
        size_t getOutputBucketNum() const;

        void next();

        bool empty() const;

    private:
        BlockInputStreamPtr stream;
        std::optional<Block> output;
        bool is_exhausted = false;
    };
    using Inputs = std::vector<Input>;
    Inputs restored_inputs;
    std::atomic_uint32_t current_bucket_num = 0;

    /// Returns true if you can abort the current task.
    Aggregator::CancellationHook is_cancelled;
};
using ExternalAggregatorPtr = std::shared_ptr<ExternalAggregator>;
} // namespace DB
