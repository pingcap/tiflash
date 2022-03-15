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

#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/IAST.h>

namespace DB
{
class ASTKillQueryQuery : public ASTQueryWithOutput
{
public:
    ASTPtr where_expression; // expression to filter processes from system.processes table
    bool sync = false; // SYNC or ASYNC mode
    bool test = false; // does it TEST mode? (doesn't cancel queries just checks and shows them)

    ASTPtr clone() const override { return std::make_shared<ASTKillQueryQuery>(*this); }

    String getID() const override;

    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

} // namespace DB
