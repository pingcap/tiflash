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

#include <Common/Logger.h>
#include <Core/SpillConfig.h>

namespace DB
{
enum class SpillStatus
{
    NOT_SPILL,
    SPILL,
};

class OperatorSpillContext
{
protected:
    UInt64 operator_spill_threshold;
    bool enable_spill = true;
    std::atomic<Int64> total_revocable_memory{0};
    LoggerPtr log;

public:
    static const Int64 INVALID_REVOCABLE_MEMORY = -1;

    OperatorSpillContext(UInt64 operator_spill_threshold_, const LoggerPtr & log_)
        : operator_spill_threshold(operator_spill_threshold_)
        , log(log_)
    {}
    virtual ~OperatorSpillContext() = default;
    bool isSpillEnabled() const { return enable_spill && operator_spill_threshold > 0; }
    void disableSpill() { enable_spill = false; }
    Int64 getTotalRevocableMemory() { return total_revocable_memory; }
};
} // namespace DB
