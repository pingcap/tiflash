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

#include <TableFunctions/ITableFunction.h>

#include <ext/singleton.h>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>


namespace DB
{
class Context;


/** Lets you get a table function by its name.
  */
class TableFunctionFactory final : public ext::Singleton<TableFunctionFactory>
{
public:
    using Creator = std::function<TableFunctionPtr()>;

    /// Register a function by its name.
    /// No locking, you must register all functions before usage of get.
    void registerFunction(const std::string & name, Creator creator);

    template <typename Function>
    void registerFunction()
    {
        registerFunction(Function::name, []() -> TableFunctionPtr { return std::make_shared<Function>(); });
    }

    /// Throws an exception if not found.
    TableFunctionPtr get(const std::string & name, const Context & context) const;

private:
    using TableFunctions = std::unordered_map<std::string, Creator>;

    TableFunctions functions;
};

} // namespace DB
