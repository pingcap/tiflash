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

#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/CommonParsers.h>

#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

#include <Common/typeid_cast.h>


namespace DB
{


bool ParserOptimizeQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_optimize_table("OPTIMIZE TABLE");
    ParserKeyword s_partition("PARTITION");
    ParserKeyword s_final("FINAL");
    ParserKeyword s_deduplicate("DEDUPLICATE");
    ParserToken s_dot(TokenType::Dot);
    ParserIdentifier name_p;
    ParserPartition partition_p;

    ASTPtr database;
    ASTPtr table;
    ASTPtr partition;
    bool final = false;
    bool deduplicate = false;

    if (!s_optimize_table.ignore(pos, expected))
        return false;

    if (!name_p.parse(pos, table, expected))
        return false;

    if (s_dot.ignore(pos, expected))
    {
        database = table;
        if (!name_p.parse(pos, table, expected))
            return false;
    }

    if (s_partition.ignore(pos, expected))
    {
        if (!partition_p.parse(pos, partition, expected))
            return false;
    }

    if (s_final.ignore(pos, expected))
        final = true;

    if (s_deduplicate.ignore(pos, expected))
        deduplicate = true;

    auto query = std::make_shared<ASTOptimizeQuery>();
    node = query;

    if (database)
        query->database = typeid_cast<const ASTIdentifier &>(*database).name;
    if (table)
        query->table = typeid_cast<const ASTIdentifier &>(*table).name;
    query->partition = partition;
    query->final = final;
    query->deduplicate = deduplicate;

    return true;
}


}
