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

namespace DB
{
/// Used to reading spilled bucket data of aggregator.
class BucketInput
{
public:
    explicit Input(const BlockInputStreamPtr & stream_);
    bool needLoad() const;
    bool load();
    Block moveOutput();
    Int32 bucketNum() const;

private:
    BlockInputStreamPtr stream;
    std::optional<Block> output;
    bool is_exhausted = false;
};
using BucketInputs = std::vector<BucketInput>;

} // namespace DB
