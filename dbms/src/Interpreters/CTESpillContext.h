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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Core/Block.h>
#include <Core/OperatorSpillContext.h>
#include <Operators/CTEPartition.h>

#include <memory>
#include <mutex>

namespace DB
{
class CTESpillContext final : public OperatorSpillContext
{
public:
    CTESpillContext(
        UInt64 operator_spill_threshold_,
        const String & query_id_and_cte_id_,
        std::vector<std::shared_ptr<CTEPartition>> partitions_)
        : OperatorSpillContext(operator_spill_threshold_, "cte", Logger::get(query_id_and_cte_id_))
        , partitions(partitions_)
    {}

    bool supportAutoTriggerSpill() const override { return true; }
    Int64 triggerSpillImpl(Int64 expected_released_memories) override;
    LoggerPtr getLog() const { return this->log; }

protected:
    Int64 getTotalRevocableMemoryImpl() override
    {
        Int64 total_memory = 0;
        for (auto & partition : this->partitions)
            total_memory += partition->memory_usage.load();
        return total_memory;
    }

private:
    std::vector<std::shared_ptr<CTEPartition>> partitions;
};
} // namespace DB
