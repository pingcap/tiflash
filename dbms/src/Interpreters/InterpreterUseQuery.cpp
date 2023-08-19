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
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterUseQuery.h>
#include <Parsers/ASTUseQuery.h>


namespace DB
{
BlockIO InterpreterUseQuery::execute()
{
    const String & new_database = typeid_cast<const ASTUseQuery &>(*query_ptr).database;
    context.getSessionContext().setCurrentDatabase(new_database);
    return {};
}

} // namespace DB
