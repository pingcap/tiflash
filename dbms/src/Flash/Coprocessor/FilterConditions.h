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

#include <common/types.h>
#include <tipb/executor.pb.h>

#include <vector>

namespace DB
{

/** This struct FilterConditions is used to store the filter conditions of the selection whose child is a table scan.
  * Those conditions will be used to construct rough index in storage engine.
  * And those conditions will be pushed down to the remote read request.
  */
struct FilterConditions
{
    static FilterConditions filterConditionsFrom(const String & executor_id, const tipb::Executor * executor);

    static FilterConditions filterConditionsFrom(const String & executor_id, const tipb::Selection & selection);

    FilterConditions() = default;

    FilterConditions(const String & executor_id_, const google::protobuf::RepeatedPtrField<tipb::Expr> & conditions_);

    bool hasValue() const { return !conditions.empty(); }

    tipb::Executor * constructSelectionForRemoteRead(tipb::Executor * mutable_executor) const;

    String executor_id;
    google::protobuf::RepeatedPtrField<tipb::Expr> conditions;
};

} // namespace DB
