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

#include <Common/typeid_cast.h>
#include <Debug/DBGInvoker.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDBGInvokeQuery.h>
#include <Parsers/ASTDBGInvokeQuery.h>

namespace DB
{
BlockIO InterpreterDBGInvokeQuery::execute()
{
    const ASTDBGInvokeQuery & ast = typeid_cast<const ASTDBGInvokeQuery &>(*query_ptr);
    BlockIO res;
    res.in = context.getDBGInvoker().invoke(context, ast.func.name, ast.func.args);
    return res;
}

} // namespace DB
