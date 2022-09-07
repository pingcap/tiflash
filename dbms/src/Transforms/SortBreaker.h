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
#include <Core/SortDescription.h>
#include <DataStreams/IBlockInputStream.h>

#include <memory>

namespace DB
{
// todo use removeConstantsFromBlock, etc to improve sort by literal like MergeSortingBlockInputStream.
class SortBreaker
{
public:
    SortBreaker(
        const SortDescription & description_,
        const String & req_id_,
        size_t max_merged_block_size_,
        size_t limit_)
        : description(description_)
        , req_id(req_id_)
        , max_merged_block_size(max_merged_block_size_)
        , limit(limit_)
    {}

    void add(Blocks && local_blocks);

    Block read();

    void initForRead(const Block & header);

    Block getHeader();

private:
    SortDescription description;
    String req_id;
    size_t max_merged_block_size;
    size_t limit;

    std::mutex mu;
    std::vector<Blocks> multi_local_blocks;
    Blocks blocks;
    std::unique_ptr<IBlockInputStream> impl;
};
using SortBreakerPtr = std::shared_ptr<SortBreaker>;
}
