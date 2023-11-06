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

#include <Parsers/IAST.h>


namespace DB
{
/** List of zero, single or multiple JOIN-ed tables or subqueries in SELECT query, with SAMPLE and FINAL modifiers.
  *
  * Table expression is:
  *  [database_name.]table_name
  * or
  *  table_function(params)
  * or
  *  (subquery)
  *
  * Optionally with alias (correllation name):
  *  [AS] alias
  *
  * Table may contain FINAL and SAMPLE modifiers:
  *  FINAL
  *  SAMPLE 1 / 10
  *  SAMPLE 0.1
  *  SAMPLE 1000000
  *
  * Table expressions may be combined with JOINs of following kinds:
  *  [GLOBAL] [ANY|ALL|] INNER|LEFT|RIGHT|FULL [OUTER] JOIN table_expr
  *  CROSS JOIN
  *  , (comma)
  *
  * In all kinds except cross and comma, there are join condition in one of following forms:
  *  USING (a, b c)
  *  USING a, b, c
  *  ON expr...
  */


/// Table expression, optionally with alias.
struct ASTTableExpression : public IAST
{
    /// One of fields is non-nullptr.
    ASTPtr database_and_table_name;
    ASTPtr table_function;
    ASTPtr subquery;

    /// Modifiers
    bool final = false;
    ASTPtr sample_size;
    ASTPtr sample_offset;

    using IAST::IAST;
    String getID() const override { return "TableExpression"; }
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


/// How to JOIN another table.
struct ASTTableJoin : public IAST
{
    /// Algorithm for distributed query processing.
    enum class Locality
    {
        Unspecified,
        Local, /// Perform JOIN, using only data available on same servers (co-located data).
        Global /// Collect and merge data from remote servers, and broadcast it to each server.
    };

    /// Allows more optimal JOIN for typical cases.
    enum class Strictness
    {
        Unspecified,
        Any, /// If there are many suitable rows to join, use any from them (also known as unique JOIN).
        All, /// If there are many suitable rows to join, use all of them and replicate rows of "left" table (usual semantic of JOIN).
    };

    /// Join method.
    enum class Kind
    {
        Inner, /// Leave ony rows that was JOINed.
        LeftOuter, /// If in "right" table there is no corresponding rows, use default values instead.
        RightOuter,
        Full,
        Cross, /// Direct product. Strictness and condition doesn't matter.
        Comma, /// Same as direct product. Intended to be converted to INNER JOIN with conditions from WHERE.
        Semi, /// semi join, return joined rows of the left table
        Anti, /// anti join, return un-joined rows of the left table
        LeftOuterSemi, /// left outer semi join, used by TiFlash, it means if row a in table A matches some rows in B, output (a, true), otherwise, output (a, false).
        LeftOuterAnti, /// anti left outer semi join, used by TiFlash, it means if row a in table A matches some rows in B, output (a, false), otherwise, output (a, true).
        Cross_LeftOuter, /// cartesian left out join, used by TiFlash
        Cross_RightOuter, /// cartesian right out join, used by TiFlash, in the implementation, it will be converted to cartesian left out join
        Cross_Semi, /// cartesian semi join, used by TiFlash
        Cross_Anti, /// cartesian anti join, used by TiFlash
        Cross_LeftOuterSemi, /// cartesian version of left outer semi join, used by TiFlash.
        Cross_LeftOuterAnti, /// cartesian version of left outer anti semi join, used by TiFlash.
        /// Note that there is no NullAware_Semi because semi join does not need to be null-aware.
        /// In semi join, if it's found in hash table, result is 1, otherwise, 0 or NULL(they're the same).
        /// However, in anti semi join, if it's found in hash table, result is 0, otherwise, 1 or NULL(they're different).
        NullAware_Anti, /// null-aware version of anti semi join, used by TiFlash.
        /// For left (anti) semi join, the exact result must be given, so both of them need to be null-aware.
        NullAware_LeftOuterSemi, /// null-aware version of left outer semi join, used by TiFlash.
        NullAware_LeftOuterAnti, /// null-aware version of left outer anti semi join, used by TiFlash.
        RightSemi, /// semi join, A semi join B, while using table A to build hash table.
        RightAnti, /// anti semi join, A anti semi join B, while using table A to build hash table.
    };

    Locality locality = Locality::Unspecified;
    Strictness strictness = Strictness::Unspecified;
    Kind kind = Kind::Inner;

    /// Condition. One of fields is non-nullptr.
    ASTPtr using_expression_list;
    ASTPtr on_expression;

    using IAST::IAST;
    String getID() const override { return "TableJoin"; }
    ASTPtr clone() const override;

    void formatImplBeforeTable(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const;
    void formatImplAfterTable(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


/// Element of list.
struct ASTTablesInSelectQueryElement : public IAST
{
    /** For first element of list, table_expression could be non-nullptr.
      * For former elements, table_join and table_expression are both non-nullptr.
      */
    ASTPtr table_join; /// How to JOIN a table, if table_expression is non-nullptr.
    ASTPtr table_expression; /// Table.

    using IAST::IAST;
    String getID() const override { return "TablesInSelectQueryElement"; }
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


/// The list. Elements are in 'children' field.
struct ASTTablesInSelectQuery : public IAST
{
    using IAST::IAST;
    String getID() const override { return "TablesInSelectQuery"; }
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


} // namespace DB
