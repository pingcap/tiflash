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

#include <Core/SpillConfig.h>
#include <Core/Spiller.h>
#include <common/types.h>


namespace DB
{
enum class SpillStatus
{
    NOT_SPILL,
    MARK_FOR_SPILL,
    SPILL,
};

class OperatorSpillContext
{
    UInt64 operator_spill_threshold;
    bool enable_spill = true;

public:
    virtual UInt64 getTotalRevocableMemory();
    virtual UInt64 markForSpill(UInt64 expected_revoke_memory);
    explicit OperatorSpillContext(UInt64 operator_spill_threshold_)
        : operator_spill_threshold(operator_spill_threshold_)
    {}
    virtual ~OperatorSpillContext() = default;
    bool isSpillEnabled() const { return enable_spill && operator_spill_threshold > 0; }
    void disableSpill() { enable_spill = false; }
};

/// used by hash join
class HashJoinSpillContext : public OperatorSpillContext
{
private:
    std::vector<std::atomic<SpillStatus>> spill_statuses;
    std::vector<std::atomic<UInt64>> revocable_memories;

public:
    HashJoinSpillContext(size_t concurrency, UInt64 operator_spill_threshold_);
};

/// used by agg
class AggSpillContext : public OperatorSpillContext
{
private:
    std::atomic<SpillStatus> spill_status{SpillStatus::NOT_SPILL};
    std::vector<std::atomic<UInt64>> revocable_memories;
    SpillConfig spill_config;
    SpillerPtr spiller;

public:
    AggSpillContext(size_t concurrency, const SpillConfig & spill_config_, UInt64 operator_spill_threshold_);
    void buildSpiller(size_t partition_num, const Block & input_schema, const LoggerPtr & log);
    SpillerPtr & getSpiller() { return spiller; }
};

/// used by non-parallel agg/sort/topn
class OrdinarySpillContext : public OperatorSpillContext
{
private:
    std::atomic<SpillStatus> spill_status{SpillStatus::NOT_SPILL};
    std::atomic<UInt64> revocable_memory{0};
};

using OperatorSpillContextPtr = std::shared_ptr<OperatorSpillContext>;

} // namespace DB
