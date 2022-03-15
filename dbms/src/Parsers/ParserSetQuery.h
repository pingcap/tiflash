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

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Query like this:
  * SET name1 = value1, name2 = value2, ...
  */
class ParserSetQuery : public IParserBase
{
public:
    explicit ParserSetQuery(bool parse_only_internals_ = false) : parse_only_internals(parse_only_internals_) {}

protected:
    const char * getName() const override { return "SET query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    /// Parse the list `name = value` pairs, without SET.
    bool parse_only_internals;
};

}
