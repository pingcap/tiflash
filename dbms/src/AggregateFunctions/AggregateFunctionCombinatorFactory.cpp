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

#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <Common/StringUtils/StringUtils.h>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

void AggregateFunctionCombinatorFactory::registerCombinator(const AggregateFunctionCombinatorPtr & value)
{
    if (!dict.emplace(value->getName(), value).second)
        throw Exception(
            "AggregateFunctionCombinatorFactory: the name '" + value->getName() + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);
}

AggregateFunctionCombinatorPtr AggregateFunctionCombinatorFactory::tryFindSuffix(const std::string & name) const
{
    /// O(N) is ok for just a few combinators.
    for (const auto & suffix_value : dict)
        if (endsWith(name, suffix_value.first))
            return suffix_value.second;
    return {};
}

} // namespace DB
