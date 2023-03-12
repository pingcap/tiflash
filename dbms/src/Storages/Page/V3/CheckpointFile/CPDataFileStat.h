// Copyright 2023 PingCAP, Ltd.
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

#include <Common/Exception.h>
#include <common/types.h>

#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace DB::PS::V3
{
struct CPFileStat
{
    size_t valid_size = 0;
    size_t total_size = 0;
};

class CPDataFilesStatCache
{
public:
    void updateValidSize(const std::unordered_map<String, size_t> & valid_sizes)
    {
        std::lock_guard lock(mtx);
        // If the file is not exist in the latest valid_sizes, then we can
        // remove the cache
        for (auto iter = stats.begin(); iter != stats.end(); /*empty*/)
        {
            const auto & file_id = iter->first;
            if (!valid_sizes.contains(file_id))
            {
                iter = stats.erase(iter);
            }
            else
            {
                ++iter;
            }
        }
        // If the file_id exists, write down the valid size, keep total_size unchanged.
        // Else create a cache entry to write down valid size but leave total_size == 0.
        for (const auto & [file_id, valid_size] : valid_sizes)
        {
            auto res = stats.try_emplace(file_id, CPFileStat{});
            res.first->second.valid_size = valid_size;
        }
    }

    void updateTotalSize(const std::unordered_map<String, CPFileStat> & total_sizes)
    {
        std::lock_guard lock(mtx);
        for (const auto & [file_id, new_stat] : total_sizes)
        {
            if (auto iter = stats.find(file_id); iter != stats.end())
            {
                assert(new_stat.total_size != 0);
                iter->second.total_size = new_stat.total_size;
            }
        }
    }

    std::unordered_map<String, CPFileStat> getCopy() const
    {
        std::lock_guard lock(mtx);
        return stats;
    }

private:
    mutable std::mutex mtx;
    std::unordered_map<String, CPFileStat> stats;
};
} // namespace DB::PS::V3
