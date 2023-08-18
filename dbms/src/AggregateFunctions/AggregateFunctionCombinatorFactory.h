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

#include <AggregateFunctions/IAggregateFunctionCombinator.h>

#include <ext/singleton.h>
#include <string>
#include <unordered_map>


namespace DB
{
/** Create aggregate function combinator by matching suffix in aggregate function name.
  */
class AggregateFunctionCombinatorFactory final : public ext::Singleton<AggregateFunctionCombinatorFactory>
{
public:
    /// Not thread safe. You must register before using tryGet.
    void registerCombinator(const AggregateFunctionCombinatorPtr & value);

    /// Example: if the name is 'avgIf', it will return combinator -If.
    AggregateFunctionCombinatorPtr tryFindSuffix(const std::string & name) const;

private:
    std::unordered_map<std::string, AggregateFunctionCombinatorPtr> dict;
};

} // namespace DB
