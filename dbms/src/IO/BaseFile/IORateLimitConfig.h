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

#include <common/logger_useful.h>
#include <common/types.h>

namespace DB
{

struct IORateLimitConfig
{
public:
    // For disk that read bandwidth and write bandwith are calculated together, such as AWS's EBS.
    UInt64 max_bytes_per_sec;
    // For disk that read bandwidth and write bandwith are calculated separatly, such as GCP's persistent disks.
    UInt64 max_read_bytes_per_sec;
    UInt64 max_write_bytes_per_sec;

    bool use_max_bytes_per_sec;

    // Currently, IORateLimiter supports 4 I/O type: foreground write, foreground read, background write and background read.
    // Initially, We calculate bandwidth for each I/O type according to the proportion of weights.
    // If *_weight is 0, the corresponding I/O type is not limited.
    UInt32 fg_write_weight;
    UInt32 bg_write_weight;
    UInt32 fg_read_weight;
    UInt32 bg_read_weight;

    Int32 emergency_pct;
    Int32 high_pct;
    Int32 medium_pct;

    Int32 tune_base;
    Int64 min_bytes_per_sec;

    Int32 auto_tune_sec;

    IORateLimitConfig()
        : max_bytes_per_sec(0)
        , max_read_bytes_per_sec(0)
        , max_write_bytes_per_sec(0)
        , use_max_bytes_per_sec(true)
        , fg_write_weight(25)
        , bg_write_weight(25)
        , fg_read_weight(25)
        , bg_read_weight(25)
        , emergency_pct(96)
        , high_pct(85)
        , medium_pct(60)
        , tune_base(2)
        , min_bytes_per_sec(2 * 1024 * 1024)
        , auto_tune_sec(5)
    {}

    void parse(const String & storage_io_rate_limit, const LoggerPtr & log);

    std::string toString() const;

    UInt64 getFgWriteMaxBytesPerSec() const;
    UInt64 getBgWriteMaxBytesPerSec() const;
    UInt64 getFgReadMaxBytesPerSec() const;
    UInt64 getBgReadMaxBytesPerSec() const;
    UInt64 getWriteMaxBytesPerSec() const;
    UInt64 getReadMaxBytesPerSec() const;
    UInt64 readWeight() const;
    UInt64 writeWeight() const;
    UInt64 totalWeight() const;

    bool operator==(const IORateLimitConfig & config) const;
};

} // namespace DB