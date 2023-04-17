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
#include <DataStreams/IBlockInputStream.h>

#include <memory>

namespace DB
{
class Aggregator;

class LocalAggregateRestorer
{
public:
    LocalAggregateRestorer(
        const BlockInputStreams & bucket_streams,
        Aggregator & aggregator_,
        std::function<bool()> is_cancelled_,
        const String & req_id);

    // load data from bucket_inputs to bucket_data.
    void loadBucketData();

    // return true if pop success
    // return false means that `loadBucketData` need to be called.
    bool tryPop(Block & block);

private:
    bool loadFromInputs();

    void storeFromInputToBucketData();

    void finish();

private:
    Aggregator & aggregator;

    std::function<bool()> is_cancelled;

    LoggerPtr log;

    bool finished = false;

    // bucket_inputs --> bucket_data --> restored_blocks.
    BlocksList bucket_data;
    BlocksList restored_blocks;

    class Input
    {
    public:
        explicit Input(const BlockInputStreamPtr & stream_);

        bool load();

        Block moveOutput();
        Int32 bucketNum() const;

    private:
        BlockInputStreamPtr stream;
        std::optional<Block> output;
        bool is_exhausted = false;
    };
    using Inputs = std::vector<Input>;
    Inputs bucket_inputs;

    static constexpr Int32 NUM_BUCKETS = 256;
};
using LocalAggregateRestorerPtr = std::unique_ptr<LocalAggregateRestorer>;
} // namespace DB
