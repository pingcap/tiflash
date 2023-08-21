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

#include <Parsers/ASTWithAlias.h>


namespace DB
{

/** Identifier (column or alias)
  */
class ASTIdentifier : public ASTWithAlias
{
public:
    enum Kind /// TODO This is semantic, not syntax. Remove it.
    {
        Column,
        Database,
        Table,
        Format,
    };

    /// name. The composite identifier here will have a concatenated name (of the form a.b.c), and individual components will be available inside the children.
    String name;

    /// what this identifier identifies
    Kind kind;

    /// if it's a cropped name it could not be an alias
    bool can_be_alias;

    ASTIdentifier(const String & name_, const Kind kind_ = Column)
        : name(name_)
        , kind(kind_)
        , can_be_alias(true)
    {}

    /** Get the text that identifies this element. */
    String getID() const override { return "Identifier_" + name; }

    ASTPtr clone() const override { return std::make_shared<ASTIdentifier>(*this); }

    void collectIdentifierNames(IdentifierNameSet & set) const override { set.insert(name); }

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame)
        const override;
    String getColumnNameImpl() const override { return name; }
};

} // namespace DB
