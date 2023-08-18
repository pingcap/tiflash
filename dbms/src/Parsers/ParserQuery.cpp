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

#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserDBGInvokeQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/ParserManageQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserUseQuery.h>

namespace DB
{
bool ParserQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserQueryWithOutput query_with_output_p;
    ParserInsertQuery insert_p(end);
    ParserUseQuery use_p;
    ParserSetQuery set_p;
    ParserDBGInvokeQuery dbginvoke_p;
    ParserManageQuery manage_p;

    bool res = query_with_output_p.parse(pos, node, expected) || insert_p.parse(pos, node, expected)
        || use_p.parse(pos, node, expected) || set_p.parse(pos, node, expected)
        || dbginvoke_p.parse(pos, node, expected) || manage_p.parse(pos, node, expected);

    return res;
}

} // namespace DB
