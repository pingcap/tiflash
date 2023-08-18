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
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTManageQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserManageQuery.h>
#include <Parsers/ParserPartition.h>


namespace DB
{
bool ParserManageQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_manage_table("MANAGE TABLE");
    ParserKeyword s_flush("FLUSH");
    ParserKeyword s_status("STATUS");
    ParserKeyword s_check("CHECK");
    ParserKeyword s_delete_rows("DELETE ROWS");
    ParserKeyword s_merge_delta("MERGE DELTA");

    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier name_p;
    ASTPtr database;
    ASTPtr table;
    ManageOperation::Enum operation;

    if (!s_manage_table.ignore(pos, expected))
        return false;

    if (!name_p.parse(pos, table, expected))
        return false;

    if (s_dot.ignore(pos, expected))
    {
        database = table;
        if (!name_p.parse(pos, table, expected))
            return false;
    }

    auto query = std::make_shared<ASTManageQuery>();
    node = query;

    if (database)
        query->database = typeid_cast<const ASTIdentifier &>(*database).name;
    if (table)
        query->table = typeid_cast<const ASTIdentifier &>(*table).name;

    if (s_flush.ignore(pos, expected))
        operation = ManageOperation::Enum::Flush;
    else if (s_status.ignore(pos, expected))
        operation = ManageOperation::Enum::Status;
    else if (s_check.ignore(pos, expected))
        operation = ManageOperation::Enum::Check;
    else if (s_merge_delta.ignore(pos, expected))
        operation = ManageOperation::Enum::MergeDelta;
    else if (s_delete_rows.ignore(pos, expected))
    {
        operation = ManageOperation::Enum::DeleteRows;
        ParserNumber num;
        ASTPtr rows;
        if (!num.parse(pos, rows, expected))
            return false;
        query->rows = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*rows).value);
    }
    else
        return false;

    query->operation = operation;

    return true;
}
} // namespace DB
