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


namespace DB
{
/* file(path, format, structure) - creates a temporary storage from file
 *
 *
 * The file must be in the clickhouse data directory.
 * The relative path begins with the clickhouse data directory.
 */
class TableFunctionFile : public ITableFunction
{
public:
    static constexpr auto name = "file";
    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context) const override;
};


} // namespace DB
