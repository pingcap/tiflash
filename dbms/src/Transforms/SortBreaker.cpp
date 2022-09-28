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

#include <DataStreams/MergeSortingBlockInputStream.h>
#include <Transforms/SortBreaker.h>

namespace DB
{
void SortBreaker::add(Blocks && local_blocks)
{
    std::lock_guard<std::mutex> lock(mu);
    multi_local_blocks.emplace_back(std::move(local_blocks));
}

Block SortBreaker::read()
{
    // TODO try lock.
    std::lock_guard<std::mutex> lock(mu);
    return impl->read();
}

void SortBreaker::initForRead()
{
    std::lock_guard<std::mutex> lock(mu);
    size_t reverse_size = 0;
    for (const auto & local_blocks : multi_local_blocks)
        reverse_size += local_blocks.size();
    blocks.reserve(reverse_size);
    for (auto & local_blocks : multi_local_blocks)
    {
        for (auto & local_block : local_blocks)
            blocks.emplace_back(std::move(local_block));
    }
    assert(header.rows() == 0);
    if (blocks.empty())
        blocks.push_back(header);
    impl = std::make_unique<MergeSortingBlocksBlockInputStream>(
        blocks,
        description,
        req_id,
        max_merged_block_size,
        limit);
}

Block SortBreaker::getHeader()
{
    assert(impl);
    return impl->getHeader();
}
} // namespace DB
