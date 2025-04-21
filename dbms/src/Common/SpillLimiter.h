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

#include <Common/TiFlashMetrics.h>

#include <mutex>

namespace DB
{
class SpillLimiter
{
public:
    explicit SpillLimiter(uint64_t max_spilled_bytes_)
        : max_spilled_bytes(max_spilled_bytes_)
    {}

    // return <ok_to_spill, current_spilled_bytes, max_spilled_bytes>
    std::tuple<bool, uint64_t, uint64_t> tryRequest(uint64_t bytes)
    {
        std::lock_guard<std::mutex> guard(lock);
        bool ok = true;
        if (max_spilled_bytes > 0)
        {
            const auto sum = current_spilled_bytes + bytes;
            // ok is true when no overflow and sum is less than max bytes.
            ok = (!(sum < current_spilled_bytes) && (sum < max_spilled_bytes));
        }

        if (ok)
        {
            current_spilled_bytes += bytes;
            GET_METRIC(tiflash_spilled_files, type_current_spilled_bytes).Set(current_spilled_bytes);
        }
        return {ok, current_spilled_bytes, max_spilled_bytes};
    }

    void release(uint64_t bytes)
    {
        std::lock_guard<std::mutex> guard(lock);
        if unlikely (current_spilled_bytes < bytes)
        {
            LOG_ERROR(
                Logger::get("SpillLimiter"),
                "unexpected release bytes({}), current bytes: {}, reset current bytes to zero",
                bytes,
                current_spilled_bytes);
            current_spilled_bytes = 0;
        }
        else
        {
            current_spilled_bytes -= bytes;
        }
        GET_METRIC(tiflash_spilled_files, type_current_spilled_bytes).Set(current_spilled_bytes);
    }

    uint64_t getCurrentSpilledBytes() const
    {
        std::lock_guard<std::mutex> guard(lock);
        return current_spilled_bytes;
    }

    void setMaxSpilledBytes(uint64_t max)
    {
        std::lock_guard<std::mutex> guard(lock);
        max_spilled_bytes = max;
    }

    static inline std::shared_ptr<SpillLimiter> instance = std::make_shared<SpillLimiter>(0);

private:
    mutable std::mutex lock;
    uint64_t max_spilled_bytes;
    uint64_t current_spilled_bytes = 0;
};

using SpillLimiterPtr = std::shared_ptr<SpillLimiter>;
} // namespace DB
