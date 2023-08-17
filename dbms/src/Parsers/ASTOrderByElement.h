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

/** Element of expression with ASC or DESC,
  *  and possibly with COLLATE.
  */
class ASTOrderByElement : public IAST
{
public:
    int direction; /// 1 for ASC, -1 for DESC
    int nulls_direction; /// Same as direction for NULLS LAST, opposite for NULLS FIRST.
    bool nulls_direction_was_explicitly_specified;

    /** Collation for locale-specific string comparison. If empty, then sorting done by bytes. */
    ASTPtr collation;

    ASTOrderByElement(
        const int direction_,
        const int nulls_direction_,
        const bool nulls_direction_was_explicitly_specified_,
        ASTPtr & collation_)
        : direction(direction_)
        , nulls_direction(nulls_direction_)
        , nulls_direction_was_explicitly_specified(nulls_direction_was_explicitly_specified_)
        , collation(collation_)
    {}

    String getID() const override { return "OrderByElement"; }

    ASTPtr clone() const override { return std::make_shared<ASTOrderByElement>(*this); }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

} // namespace DB
