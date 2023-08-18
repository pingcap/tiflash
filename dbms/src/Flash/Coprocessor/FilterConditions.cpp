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

#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/FilterConditions.h>
#include <common/likely.h>

namespace DB
{
FilterConditions::FilterConditions(
    const String & executor_id_,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & conditions_)
    : executor_id(executor_id_)
    , conditions(conditions_)
{
    if (unlikely(conditions.empty() != executor_id.empty()))
    {
        throw TiFlashException(
            "for FilterConditions, conditions and executor_id should both be empty or neither should be empty",
            Errors::Coprocessor::BadRequest);
    }
}

tipb::Executor * FilterConditions::constructSelectionForRemoteRead(tipb::Executor * mutable_executor) const
{
    if (hasValue())
    {
        mutable_executor->set_tp(tipb::ExecType::TypeSelection);
        mutable_executor->set_executor_id(executor_id);
        auto * new_selection = mutable_executor->mutable_selection();
        for (const auto & condition : conditions)
            *new_selection->add_conditions() = condition;
        return new_selection->mutable_child();
    }
    else
    {
        return mutable_executor;
    }
}

FilterConditions FilterConditions::filterConditionsFrom(const String & executor_id, const tipb::Executor * executor)
{
    if (!executor || !executor->has_selection())
    {
        return {"", {}};
    }

    return filterConditionsFrom(executor_id, executor->selection());
}

FilterConditions FilterConditions::filterConditionsFrom(const String & executor_id, const tipb::Selection & selection)
{
    return {executor_id, selection.conditions()};
}
} // namespace DB
