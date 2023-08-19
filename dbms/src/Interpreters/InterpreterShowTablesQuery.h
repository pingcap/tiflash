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

#include <Interpreters/IInterpreter.h>


namespace DB
{
class Context;
class IAST;
using ASTPtr = std::shared_ptr<IAST>;


/** Return a list of tables or databases meets specified conditions.
  * Interprets a query through replacing it to SELECT query from system.tables or system.databases.
  */
class InterpreterShowTablesQuery : public IInterpreter
{
public:
    InterpreterShowTablesQuery(const ASTPtr & query_ptr_, Context & context_);

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    Context & context;

    String getRewrittenQuery();
};


} // namespace DB
