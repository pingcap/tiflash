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

#include <memory>
#include <string>


namespace DB
{
class Context;
class IAST;
using ASTPtr = std::shared_ptr<IAST>;
class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;


/** Interface for table functions.
  *
  * Table functions are not relevant to other functions.
  * The table function can be specified in the FROM section instead of the [db.]Table
  * The table function returns a temporary StoragePtr object that is used to execute the query.
  *
  * Example:
  * SELECT count() FROM remote('example01-01-1', merge, hits)
  * - go to `example01-01-1`, in `merge` database, `hits` table.
  */

class ITableFunction
{
public:
    /// Get the main function name.
    virtual std::string getName() const = 0;

    /// Create storage according to the query.
    StoragePtr execute(const ASTPtr & ast_function, const Context & context) const;

    virtual ~ITableFunction() = default;

private:
    virtual StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context) const = 0;
};

using TableFunctionPtr = std::shared_ptr<ITableFunction>;


} // namespace DB
