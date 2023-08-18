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

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

/** Single SELECT query or multiple SELECT queries with UNION ALL.
  * Only UNION ALL is possible. No UNION DISTINCT or plain UNION.
  */
class ASTSelectWithUnionQuery : public ASTQueryWithOutput
{
public:
    String getID() const override { return "SelectWithUnionQuery"; }

    ASTPtr clone() const override;
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    void setDatabaseIfNeeded(const String & database_name);

    ASTPtr list_of_selects;
};

} // namespace DB
