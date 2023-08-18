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

#include <Parsers/formatAST.h>
#include <Parsers/queryToString.h>

#include <sstream>

namespace DB
{
String queryToString(const ASTPtr & query)
{
    return queryToString(*query);
}

String queryToString(const IAST & query)
{
    std::ostringstream out;
    formatAST(query, out, false, true);
    return out.str();
}
} // namespace DB
