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

#include <Flash/Pipeline/Schedule/Tasks/PipeConditionVariable.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>

namespace DB
{
enum class CTEOpStatus
{
    OK,
    BLOCK_NOT_AVAILABLE,
    END_OF_FILE,
    CANCELLED,
    SINK_NOT_REGISTERED
};

struct CTEPartition
{
    std::unique_ptr<std::mutex> mu;
    Blocks blocks;
    std::vector<size_t> fetch_block_idxs;
    size_t memory_usages = 0;
    std::unique_ptr<PipeConditionVariable> pipe_cv;

    void tryToDeleteBlockNoLock(size_t idx)
    {
        bool need_delete = true;
        for (auto item : this->fetch_block_idxs)
        {
            if (item <= idx)
            {
                need_delete = false;
                break;
            }
        }

        if (need_delete)
            this->blocks[idx].clear();
    }
};
} // namespace DB
