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
#include <common/StringRef.h>


namespace DB
{

/// Either a (possibly compound) expression representing a partition value or a partition ID.
class ASTPartition : public IAST
{
public:
    ASTPtr value;
    StringRef fields_str; /// The extent of comma-separated partition expression fields without parentheses.
    size_t fields_count = 0;

    String id;

    String getID() const override;
    ASTPtr clone() const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

} // namespace DB
