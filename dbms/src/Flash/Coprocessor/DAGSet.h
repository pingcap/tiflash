// Copyright 2022 PingCAP, Ltd.
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

#include <tipb/expression.pb.h>

#include <memory>
#include <utility>

namespace DB
{
class Set;
using SetPtr = std::shared_ptr<Set>;

struct DAGSet
{
    DAGSet(SetPtr constant_set_, std::vector<const tipb::Expr *> remaining_exprs_)
        : constant_set(std::move(constant_set_))
        , remaining_exprs(std::move(remaining_exprs_)){};
    SetPtr constant_set;
    std::vector<const tipb::Expr *> remaining_exprs;
};
} // namespace DB
