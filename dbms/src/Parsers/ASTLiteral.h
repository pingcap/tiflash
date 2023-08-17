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

#include <Common/FieldVisitors.h>
#include <Core/Field.h>
#include <Parsers/ASTWithAlias.h>


namespace DB
{

/** Literal (atomic) - number, string, NULL
  */
class ASTLiteral : public ASTWithAlias
{
public:
    Field value;

    ASTLiteral(const Field & value_)
        : value(value_)
    {}

    /** Get the text that identifies this element. */
    String getID() const override { return "Literal_" + applyVisitor(FieldVisitorDump(), value); }

    ASTPtr clone() const override { return std::make_shared<ASTLiteral>(*this); }

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const override
    {
        settings.ostr << applyVisitor(FieldVisitorToString(), value);
    }

    String getColumnNameImpl() const override;
};

} // namespace DB
