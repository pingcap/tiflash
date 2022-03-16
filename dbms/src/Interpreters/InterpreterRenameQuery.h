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


namespace DB
{
class Context;
class IAST;
using ASTPtr = std::shared_ptr<IAST>;


/** Rename one table
  *  or rename many tables at once.
  */
class InterpreterRenameQuery : public IInterpreter
{
public:
    InterpreterRenameQuery(const ASTPtr & query_ptr_, Context & context_, const String executor_name_);
    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    Context & context;
    const String executor_name;
};


} // namespace DB
