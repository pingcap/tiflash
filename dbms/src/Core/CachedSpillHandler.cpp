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

#include <Core/CachedSpillHandler.h>
#include <DataStreams/IBlockInputStream.h>

namespace DB
{
CachedSpillHandler::CachedSpillHandler(
    Spiller * spiller,
    UInt64 partition_id,
    const BlockInputStreamPtr & from_,
    size_t bytes_threshold_,
    const std::function<bool()> & is_cancelled_)
    : handler(spiller->createSpillHandler(partition_id))
    , from(from_)
    , bytes_threshold(bytes_threshold_)
    , is_cancelled(is_cancelled_)
{
    assert(from);
    from->readPrefix();
}

/// bytes_threshold == 0 means no limit, and will read all data
bool CachedSpillHandler::batchRead()
{
    assert(batch.empty());

    std::vector<Block> ret;
    size_t current_return_size = 0;
    while (Block block = from->read())
    {
        if unlikely (is_cancelled())
            return false;
        if unlikely (block.rows() == 0)
            continue;
        ret.push_back(std::move(block));
        current_return_size += ret.back().estimateBytesForSpill();
        if (bytes_threshold > 0 && current_return_size >= bytes_threshold)
            break;
    }

    if unlikely (is_cancelled())
        return false;

    if (ret.empty())
    {
        assert(!finished);
        finished = true;

        from->readSuffix();
        /// submit the spilled data
        handler.finish();
        return false;
    }
    else
    {
        std::swap(batch, ret);
        return true;
    }
}

void CachedSpillHandler::spill()
{
    assert(!finished && !batch.empty());
    std::vector<Block> ret;
    std::swap(batch, ret);
    handler.spillBlocks(std::move(ret));
}
} // namespace DB
