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

#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/ColumnNumbers.h>
#include <Core/Names.h>


namespace DB
{
struct AggregateDescription
{
    AggregateFunctionPtr function;
    Array parameters; /// Parameters of the (parametric) aggregate function.
    ColumnNumbers arguments;
    Names argument_names; /// used if no `arguments` are specified.
    String column_name; /// What name to use for a column with aggregate function values
};

using AggregateDescriptions = std::vector<AggregateDescription>;
using KeyRefAggFuncMap = std::unordered_map<String, String>;
using AggFuncRefKeyMap = std::unordered_map<String, String>;

} // namespace DB
