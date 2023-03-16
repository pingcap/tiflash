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

#include <common/types.h>
#include <fmt/format.h>

#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace DB::PS::V3
{

using RemoteFileValidSizes = std::unordered_map<String, size_t>;

struct CPDataFileStat
{
    Int64 valid_size = 0;
    Int64 total_size = 0;
};

class CPDataFilesStatCache
{
public:
    /**
     * Update the valid size in the cache. Thread safe.
     *
     * If some file_id exist in `valid_sizes` but not exist in the cache, it
     * will create a new `CPDataFileStat` with total_size == 0.
     * If some file_id exist in the cache but not exist in the `valid_sizes`,
     * it means the data file is not owned by this node any more, so it will
     * be removed from the cache.
     */
    void updateValidSize(const RemoteFileValidSizes & valid_sizes);

    /**
     * Update the total size field in the cache. Thread safe.
     */
    void updateTotalSize(const std::unordered_map<String, CPDataFileStat> & total_sizes);

    std::unordered_map<String, CPDataFileStat> getCopy() const
    {
        std::lock_guard lock(mtx);
        return stats;
    }

private:
    mutable std::mutex mtx;
    // file_id -> CPDataFileStat
    std::unordered_map<String, CPDataFileStat> stats;
};
} // namespace DB::PS::V3

template <>
struct fmt::formatter<DB::PS::V3::CPDataFileStat>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::PS::V3::CPDataFileStat & value, FormatContext & ctx) const -> decltype(ctx.out())
    {
        return format_to(ctx.out(), "valid:{},total:{}", value.valid_size, value.total_size);
    }
};
