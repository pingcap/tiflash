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
struct PushDownFilter
{
    static PushDownFilter pushDownFilterFrom(const String & executor_id, const tipb::Executor * executor);

    static PushDownFilter pushDownFilterFrom(const String & executor_id, const tipb::Selection & selection);

    PushDownFilter() = default;

    PushDownFilter(
        const String & executor_id_,
        const std::vector<const tipb::Expr *> & conditions_);

    bool hasValue() const { return !conditions.empty(); }

    tipb::Executor * constructSelectionForRemoteRead(tipb::Executor * mutable_executor) const;

    String executor_id;
    std::vector<const tipb::Expr *> conditions;
};
} // namespace DB
