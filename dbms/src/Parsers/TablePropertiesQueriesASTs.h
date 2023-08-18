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

#include <Parsers/ASTQueryWithTableAndOutput.h>


namespace DB
{
struct ASTExistsQueryIDAndQueryNames
{
    static constexpr auto ID = "ExistsQuery";
    static constexpr auto Query = "EXISTS TABLE";
};

struct ASTDescribeQueryExistsQueryIDAndQueryNames
{
    static constexpr auto ID = "DescribeQuery";
    static constexpr auto Query = "DESCRIBE TABLE";
};

using ASTExistsQuery = ASTQueryWithTableAndOutputImpl<ASTExistsQueryIDAndQueryNames>;

class ASTDescribeQuery : public ASTQueryWithOutput
{
public:
    ASTPtr table_expression;

    String getID() const override { return "DescribeQuery"; };

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTDescribeQuery>(*this);
        res->children.clear();
        if (table_expression)
        {
            res->table_expression = table_expression->clone();
            res->children.push_back(res->table_expression);
        }
        cloneOutputOptions(*res);
        return res;
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "DESCRIBE TABLE "
                      << (settings.hilite ? hilite_none : "");
        table_expression->formatImpl(settings, state, frame);
    }
};

} // namespace DB
