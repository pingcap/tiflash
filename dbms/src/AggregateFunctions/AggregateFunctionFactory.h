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

#include <ext/singleton.h>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>


namespace DB
{
class Context;
class IDataType;

using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;


/** Creates an aggregate function by name.
  */
class AggregateFunctionFactory final : public ext::Singleton<AggregateFunctionFactory>
{
    friend class StorageSystemFunctions;

public:
    /** Creator have arguments: name of aggregate function, types of arguments, values of parameters.
      * Parameters are for "parametric" aggregate functions.
      * For example, in quantileWeighted(0.9)(x, weight), 0.9 is "parameter" and x, weight are "arguments".
      */
    using Creator = std::function<AggregateFunctionPtr(const Context &, const String &, const DataTypes &, const Array &)>;

    /// For compatibility with SQL, it's possible to specify that certain aggregate function name is case insensitive.
    enum CaseSensitiveness
    {
        CaseSensitive,
        CaseInsensitive
    };

    /// Register a function by its name.
    /// No locking, you must register all functions before usage of get.
    void registerFunction(const String & name, Creator creator, CaseSensitiveness case_sensitiveness = CaseSensitive);

    /// Throws an exception if not found.
    AggregateFunctionPtr get(
        const Context & context,
        const String & name,
        const DataTypes & argument_types,
        const Array & parameters = {},
        int recursion_level = 0,
        bool empty_input_as_null = false) const;

    /// Returns nullptr if not found.
    AggregateFunctionPtr tryGet(
        const Context & context,
        const String & name,
        const DataTypes & argument_types,
        const Array & parameters = {}) const;

    bool isAggregateFunctionName(const String & name, int recursion_level = 0) const;

private:
    AggregateFunctionPtr getImpl(
        const Context & context,
        const String & name,
        const DataTypes & argument_types,
        const Array & parameters,
        int recursion_level) const;

private:
    using AggregateFunctions = std::unordered_map<String, Creator>;

    AggregateFunctions aggregate_functions;

    /// Case insensitive aggregate functions will be additionally added here with lowercased name.
    AggregateFunctions case_insensitive_aggregate_functions;
};

} // namespace DB
