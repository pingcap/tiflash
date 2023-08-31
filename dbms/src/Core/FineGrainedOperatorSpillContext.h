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

#include <Core/OperatorSpillContext.h>

namespace DB
{
class FineGrainedOperatorSpillContext : public OperatorSpillContext
{
protected:
    std::vector<OperatorSpillContextPtr> operator_spill_contexts;

    Int64 getTotalRevocableMemoryImpl() override;

public:
    FineGrainedOperatorSpillContext(UInt64 operator_spill_threshold, const String op_name, const LoggerPtr & log)
        : OperatorSpillContext(operator_spill_threshold, op_name, log)
    {}
    bool isSpillEnabled() const override;
    bool supportFurtherSpill() const override;
    bool supportAutoTriggerSpill() const override { return true; }
    Int64 triggerSpillImpl(Int64 expected_released_memories) override;
    void addOperatorSpillContext(const OperatorSpillContextPtr & operator_spill_context);
};
} // namespace DB