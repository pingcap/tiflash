// Copyright 2025 PingCAP, Inc.
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

#include <Core/CTESpill.h>
#include <fmt/core.h>

#include <mutex>
#include <shared_mutex>
#include <utility>

namespace DB
{
void CTESpill::writeBlocks(const Blocks & blocks)
{
    std::unique_lock<std::shared_mutex> lock(this->rw_lock);

    for (const auto & block : blocks)
    {
        if unlikely (this->spilled_files.back().isFull() || this->spilled_files.empty())
        {
            // TODO create new spilled_files and write_read_files
        }

        Int64 prev_block_offset;
        Int64 current_block_file_idx = this->spilled_files.size() - 1;
        if unlikely (this->block_offsets.empty() || this->block_offsets.back().first != current_block_file_idx)
            prev_block_offset = 0;
        else
            prev_block_offset = this->block_offsets.back().second;

        const auto block_size = this->write_streams.back().writeAndReturnBlockSize(block);
        this->block_offsets.push_back((std::make_pair(this->spilled_files.size() - 1, prev_block_offset + block_size)));
    }
}

Block CTESpill::readBlockAt(Int64 idx)
{
    std::shared_lock<std::shared_mutex> lock(this->rw_lock);
    if unlikely (idx >= static_cast<Int64>(this->block_offsets.size()))
        throw Exception(fmt::format(
            "Requested block idx({}) is larger than total block number({})",
            idx,
            this->block_offsets.size()));

    auto block_location = this->block_offsets[idx];
    Int64 stream_idx = block_location.first;
    Int64 block_offset = block_location.second;
    Int64 block_size = this->getBlockSizeNoLock(idx);

    // TODO why we resize buf? Maybe we can delete this code.
    if (this->buf.size() < static_cast<size_t>(block_size))
        this->buf.resize(block_size);

    this->read_streams[stream_idx].seek(block_offset);
    return this->read_streams[stream_idx].read();
}

Int64 CTESpill::blockNum()
{
    std::shared_lock<std::shared_mutex> lock(this->rw_lock);
    return this->block_offsets.size();
}

Int64 CTESpill::getBlockSizeNoLock(Int64 idx) const
{
    if unlikely (idx >= static_cast<Int64>(this->block_offsets.size()))
        throw Exception(fmt::format(
            "Requested block idx({}) is larger than total block number({})",
            idx,
            this->block_offsets.size()));

    if unlikely (idx == 0)
        return this->block_offsets[0].second;

    Int64 prev_block_file_idx = this->block_offsets[idx - 1].first;
    if unlikely (prev_block_file_idx != this->block_offsets[idx].first)
        return this->block_offsets[idx].second;

    return this->block_offsets[idx].second - this->block_offsets[idx - 1].second;
}
} // namespace DB
