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

#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>

namespace DB
{
/// Used to reading spilled bucket data of aggregator.
class SpilledBucketInput;
using SpilledBucketInputs = std::vector<SpilledBucketInput>;
class SpilledBucketInput
{
public:
    explicit SpilledBucketInput(const BlockInputStreamPtr & stream_);

    bool needLoad() const;
    void load();

    bool hasOutput() const;
    Int32 bucketNum() const;
    Block popOutput();

    static Int32 getMinBucketNum(const SpilledBucketInputs & inputs);

    static BlocksList popOutputs(SpilledBucketInputs & inputs, Int32 target_bucket_num);

public:
    static constexpr Int32 NUM_BUCKETS = 256;

private:
    BlockInputStreamPtr stream;
    std::optional<Block> output;
    bool is_exhausted = false;
};

} // namespace DB
