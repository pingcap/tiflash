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

#include <Parsers/IParserBase.h>


namespace DB
{

/** List of single or multiple JOIN-ed tables or subqueries in SELECT query, with ARRAY JOINs and SAMPLE, FINAL modifiers.
  */
class ParserTablesInSelectQuery : public IParserBase
{
protected:
    const char * getName() const { return "table, table function, subquery or list of joined tables"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


class ParserTablesInSelectQueryElement : public IParserBase
{
public:
    ParserTablesInSelectQueryElement(bool is_first) : is_first(is_first) {}

protected:
    const char * getName() const { return "table, table function, subquery or list of joined tables"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);

private:
    bool is_first;
};


class ParserTableExpression : public IParserBase
{
protected:
    const char * getName() const { return "table or subquery or table function"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


class ParserArrayJoin : public IParserBase
{
protected:
    const char * getName() const { return "array join"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


}
