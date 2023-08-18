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

#include <Core/Names.h>
#include <Parsers/IAST.h>


namespace DB
{

struct ASTTablesInSelectQueryElement;


/** SELECT query
  */
class ASTSelectQuery : public IAST
{
public:
    /** Get the text that identifies this element. */
    String getID() const override { return "SelectQuery"; };

    ASTPtr clone() const override;

    bool raw_for_mutable = false;
    bool distinct = false;
    bool no_kvstore = false;
    ASTPtr with_expression_list;
    ASTPtr select_expression_list;
    ASTPtr tables;
    ASTPtr partition_expression_list;
    ASTPtr segment_expression_list;
    ASTPtr prewhere_expression;
    ASTPtr where_expression;
    ASTPtr group_expression_list;
    ASTPtr having_expression;
    ASTPtr order_expression_list;
    ASTPtr limit_by_value;
    ASTPtr limit_by_expression_list;
    ASTPtr limit_offset;
    ASTPtr limit_length;
    ASTPtr settings;

    /// Compatibility with old parser of tables list. TODO remove
    ASTPtr database() const;
    ASTPtr table() const;
    ASTPtr sample_size() const;
    ASTPtr sample_offset() const;
    const ASTTablesInSelectQueryElement * join() const;
    bool final() const;
    void setDatabaseIfNeeded(const String & database_name);
    void replaceDatabaseAndTable(const String & database_name, const String & table_name);

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

} // namespace DB
