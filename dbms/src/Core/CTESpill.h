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

#pragma once

#include <Core/SpillConfig.h>
#include <Core/Spiller.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <IO/BaseFile/WriteReadableFile.h>
#include <common/types.h>

#include <shared_mutex>
#include <utility>
#include <vector>

namespace DB
{
class CTESpill
{
public:
    // TODO maybe we need an initialization function as spill may not be triggered and we can initialize it until spill is triggered

    // TODO all function need lock as CTESpill may be concurrently accessed
    void writeBlocks(const Blocks & blocks); // TODO implement, return value need other types
    Block readBlockAt(Int64 idx);
    Int64 blockNum();

private:
    Int64 getBlockSizeNoLock(Int64 idx) const;

    std::shared_mutex rw_lock;
    std::vector<SpilledFile> spilled_files;
    // TODO maybe we need to use compressed stream
    // TODO maybe each one stream need one lock
    std::vector<NativeBlockInputStream> read_streams;
    std::vector<NativeBlockOutputStream> write_streams;
    std::vector<std::pair<Int64, Int64>> block_offsets;
    FileProviderPtr file_provider;
    Int64 write_offset; // Always write to the last file, so we need only one offset
    SpillConfig config; // TODO initialize it

    std::vector<char> buf;
};
} // namespace DB
