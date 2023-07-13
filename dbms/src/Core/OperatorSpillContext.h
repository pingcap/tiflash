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
    std::atomic<bool> in_spillable_stage{true};
    bool enable_spill = true;
    LoggerPtr log;

    virtual Int64 getTotalRevocableMemoryImpl() = 0;

public:
    OperatorSpillContext(UInt64 operator_spill_threshold_, const LoggerPtr & log_)
        : operator_spill_threshold(operator_spill_threshold_)
        , log(log_)
    {}
    virtual ~OperatorSpillContext() = default;
    bool isSpillEnabled() const { return enable_spill && operator_spill_threshold > 0; }
    void disableSpill() { enable_spill = false; }
    void finishSpillableStage() { in_spillable_stage = false; }
    Int64 getTotalRevocableMemory()
    {
        if (in_spillable_stage)
            return getTotalRevocableMemoryImpl();
        else
            return 0;
    }
    UInt64 getOperatorSpillThreshold() const { return operator_spill_threshold; }
};
} // namespace DB
