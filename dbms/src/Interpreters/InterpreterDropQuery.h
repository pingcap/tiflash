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

#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTDropQuery.h>


namespace DB
{
class Context;
class IAST;
using ASTPtr = std::shared_ptr<IAST>;


/** Allow to either drop table with all its data (DROP), or remove information about table (just forget) from server (DETACH).
  */
class InterpreterDropQuery : public IInterpreter
{
public:
    InterpreterDropQuery(const ASTPtr & query_ptr_, Context & context_);

    /// Drop table or database.
    BlockIO execute() override;

private:
    void checkAccess(const ASTDropQuery & drop);
    ASTPtr query_ptr;
    Context & context;
};
} // namespace DB
