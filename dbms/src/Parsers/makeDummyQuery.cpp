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

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/makeDummyQuery.h>
#include <Parsers/parseQuery.h>

namespace DB
{

ASTPtr makeDummyQuery()
{
    static const String tmp = "select 1";
    ParserQuery parser(tmp.data() + tmp.size());
    ASTPtr parent = parseQuery(parser, tmp.data(), tmp.data() + tmp.size(), "", 0);
    return ((ASTSelectWithUnionQuery *)parent.get())->list_of_selects->children.at(0);
}

} // namespace DB
