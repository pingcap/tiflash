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

#include <Common/Logger.h>
#include <Core/SpillConfig.h>
#include <common/logger_useful.h>

namespace DB
{
enum class AutoSpillStatus
{
    /// NO_NEED_AUTO_SPILL means auto spill is not needed or current auto spill already finished
    NO_NEED_AUTO_SPILL,
    /// WAIT_SPILL_FINISH means the operator is aware that it needs spill, but spill does not finish yet
    WAIT_SPILL_FINISH,
    /// NEED_AUTO_SPILL means to mark the operator to spill, the operator itself may not aware that it needs spill yet
    NEED_AUTO_SPILL,
};

class OperatorSpillContext
{
protected:
    UInt64 operator_spill_threshold;
    std::atomic<bool> is_spilled{false};
    std::atomic<bool> in_spillable_stage{true};
    bool enable_spill = true;
    bool auto_spill_mode = false;
    String op_name;
    LoggerPtr log;

    virtual Int64 getTotalRevocableMemoryImpl() = 0;

public:
    /// minimum revocable operator memories that will trigger a spill
    const static Int64 MIN_SPILL_THRESHOLD = 10ULL * 1024 * 1024;
    OperatorSpillContext(UInt64 operator_spill_threshold_, const String op_name_, const LoggerPtr & log_)
        : operator_spill_threshold(operator_spill_threshold_)
        , op_name(op_name_)
        , log(log_)
    {}
    virtual ~OperatorSpillContext() = default;
    bool isSpillEnabled() const;
    bool supportSpill() const;
    void disableSpill() { enable_spill = false; }
    virtual bool supportFurtherSpill() const { return in_spillable_stage; }
    bool isInAutoSpillMode() const { return auto_spill_mode; }
    void setAutoSpillMode();
    Int64 getTotalRevocableMemory();
    UInt64 getOperatorSpillThreshold() const { return operator_spill_threshold; }
    void markSpilled();
    bool isSpilled() const { return is_spilled; }
    /// auto trigger spill means the operator will auto spill under the constraint of query/global level memory threshold,
    /// so user does not need set operator_spill_threshold explicitly
    virtual bool supportAutoTriggerSpill() const { return false; }
    Int64 triggerSpill(Int64 expected_released_memories);
    virtual Int64 triggerSpillImpl(Int64 expected_released_memories) = 0;
    void finishSpillableStage();
};

using OperatorSpillContextPtr = std::shared_ptr<OperatorSpillContext>;
using RegisterOperatorSpillContext = std::function<void(const OperatorSpillContextPtr & ptr)>;
} // namespace DB
