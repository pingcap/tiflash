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

#include <ostream>


namespace DB
{

/** Takes a syntax tree and turns it back into text.
  * In case of INSERT query, the data will be missing.
  */
void formatAST(const IAST & ast, std::ostream & s, bool hilite = true, bool one_line = false);

inline std::ostream & operator<<(std::ostream & os, const IAST & ast)
{
    return formatAST(ast, os, false, true), os;
}
inline std::ostream & operator<<(std::ostream & os, const ASTPtr & ast)
{
    return formatAST(*ast, os, false, true), os;
}

} // namespace DB
