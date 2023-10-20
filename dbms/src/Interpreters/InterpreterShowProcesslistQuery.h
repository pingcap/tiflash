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


/** Return list of currently executing queries.
  */
class InterpreterShowProcesslistQuery : public IInterpreter
{
public:
    InterpreterShowProcesslistQuery(const ASTPtr & query_ptr_, Context & context_)
        : query_ptr(query_ptr_)
        , context(context_)
    {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    Context & context;
};


} // namespace DB
