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

#include <Core/Field.h>
#include <Core/Types.h>
#include <WindowFunctions/IWindowFunction.h>

#include <ext/singleton.h>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB
{
class IDataType;

using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;

/** Creates an window function by name.
  */
class WindowFunctionFactory final : public ext::Singleton<WindowFunctionFactory>
{
public:
    using Creator = std::function<WindowFunctionPtr(const DataTypes &)>;

    /// Register a function by its name.
    /// No locking, you must register all functions before usage of get.
    void registerFunction(const String & name, Creator creator);

    template <typename Function>
    void registerFunction()
    {
        registerFunction(Function::name, [](const DataTypes & argument_types) {
            return std::make_shared<Function>(argument_types);
        });
    }

    /// Throws an exception if not found.
    WindowFunctionPtr get(const String & name, const DataTypes & argument_types) const;

    /// Returns nullptr if not found.
    WindowFunctionPtr tryGet(const String & name, const DataTypes & argument_types) const;

    bool isWindowFunctionName(const String & name) const;

private:
    WindowFunctionPtr getImpl(const String & name, const DataTypes & argument_types) const;

private:
    using WindowFunctions = std::unordered_map<String, Creator>;

    WindowFunctions window_functions;
};

} // namespace DB
