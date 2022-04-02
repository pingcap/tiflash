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

#include <Parsers/IAST.h>


namespace DB
{


/** DELETE query
  */
class ASTDeleteQuery : public IAST
{
public:
    String database;
    String table;
    ASTPtr partition_expression_list;
    ASTPtr where;

    // Just for execute.
    ASTPtr select;

    /** Get the text that identifies this element. */
    String getID() const override { return "DeleteQuery_" + database + "_" + table; };

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTDeleteQuery>(*this);
        res->children.clear();

        if (where)
        {
            res->where = where->clone();
            res->children.push_back(res->where);
        }

        return res;
    }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
