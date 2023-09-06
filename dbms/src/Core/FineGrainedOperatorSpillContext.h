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
/// FineGrainedOperatorSpillContext is a wrap for all the operator spill contexts that belongs to a same executor
/// with fine grained shuffle enable. When fine grained shuffle is enabled, executors like sort and aggregation
/// will have n independent operators in TiFlash side, each of them have their own operator spill context. However
/// these n operators are not really independent: they share the same source operator(exchange receiver), if any one
/// of the operator stop consuming data from exchange receiver(for example, begin spill data), all the others will
/// be stuck because the exchange receiver has a bounded queue for all the operators, any one of the operator stop
/// consuming will make the queue full, and no other data can be pushed to exchange receiver. If all the n operator
/// is triggered to spill serially, it will affects the overall performance seriously.
/// FineGrainedOperatorSpillContext is used to make sure that all the operators belongs to the same executor
/// will be triggered to spill almost at the same time
class FineGrainedOperatorSpillContext final : public OperatorSpillContext
{
private:
    std::vector<OperatorSpillContextPtr> operator_spill_contexts;

protected:
    Int64 getTotalRevocableMemoryImpl() override;

public:
    FineGrainedOperatorSpillContext(const String op_name, const LoggerPtr & log)
        : OperatorSpillContext(0, op_name, log)
    {
        auto_spill_mode = true;
    }
    bool supportFurtherSpill() const override;
    bool supportAutoTriggerSpill() const override { return true; }
    Int64 triggerSpillImpl(Int64 expected_released_memories) override;
    void addOperatorSpillContext(const OperatorSpillContextPtr & operator_spill_context);
    /// only for test
    size_t getOperatorSpillCount() const { return operator_spill_contexts.size(); }
};
} // namespace DB