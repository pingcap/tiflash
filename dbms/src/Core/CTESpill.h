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
#include <common/types.h>

#include <shared_mutex>
#include <utility>
#include <vector>

namespace DB
{
// TODO we can use WriteBufferFromWritableFile(used SpillHandler::SpillWriter) to write file
// TODO we can use ReadBufferFromRandomAccessFile to write file
class CTESpill
{
public:
    void writeBlocks(Blocks && blocks); // TODO implement
    void readBlockAt(Int64 idx) const; // TODO implement
    Int64 blockNum() const; // TODO implement

private:
    std::shared_mutex rw_lock;
    std::vector<std::pair<Int64, Int64>> block_offsets;
    std::vector<SpilledFile> spilled_files;
    SpillConfig config; // TODO initialize it
    
    // TODO maybe we need a queue to transfer block
};
} // namespace DB
