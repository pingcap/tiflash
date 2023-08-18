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

#include <Core/SpillHandler.h>

namespace DB
{
class CachedSpillHandler
{
public:
    CachedSpillHandler(
        Spiller * spiller,
        UInt64 partition_id,
        const BlockInputStreamPtr & from_,
        size_t bytes_threshold_,
        const std::function<bool()> & is_cancelled_);

    bool batchRead();

    void spill();

private:
    SpillHandler handler;
    BlockInputStreamPtr from;
    size_t bytes_threshold;
    std::function<bool()> is_cancelled;

    Blocks batch;
    bool finished = false;
};
using CachedSpillHandlerPtr = std::shared_ptr<CachedSpillHandler>;

} // namespace DB
