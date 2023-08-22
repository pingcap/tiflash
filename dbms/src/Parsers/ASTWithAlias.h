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


/** Base class for AST, which can contain an alias (identifiers, literals, functions).
  */
class ASTWithAlias : public IAST
{
public:
    /// The alias, if any, or an empty string.
    String alias;
    /// If is true, getColumnName returns alias. Uses for aliases in former WITH section of SELECT query.
    /// Example: 'WITH pow(2, 2) as a SELECT pow(a, 2)' returns 'pow(a, 2)' instead of 'pow(pow(2, 2), 2)'
    bool prefer_alias_to_column_name = false;

    using IAST::IAST;

    String getColumnName() const override final
    {
        return prefer_alias_to_column_name && !alias.empty() ? alias : getColumnNameImpl();
    }
    String getAliasOrColumnName() const override { return alias.empty() ? getColumnNameImpl() : alias; }
    String tryGetAlias() const override { return alias; }
    void setAlias(const String & to) override { alias = to; }

    /// Calls formatImplWithoutAlias, and also outputs an alias. If necessary, encloses the entire expression in brackets.
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame)
        const override final;

    virtual void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame)
        const = 0;

protected:
    virtual String getColumnNameImpl() const = 0;
};

/// helper for setting aliases and chaining result to other functions
inline ASTPtr setAlias(ASTPtr ast, const String & alias)
{
    ast->setAlias(alias);
    return ast;
};


} // namespace DB
