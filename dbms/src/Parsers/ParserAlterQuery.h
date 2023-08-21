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

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IParserBase.h>

namespace DB
{

/** Query like this:
  * ALTER TABLE [db.]name [ON CLUSTER cluster]
  *     [ADD COLUMN col_name type [AFTER col_after],]
  *     [DROP COLUMN col_to_drop, ...]
  *     [CLEAR COLUMN col_to_clear [IN PARTITION partition],]
  *     [MODIFY COLUMN col_to_modify type, ...]
  *     [MODIFY PRIMARY KEY (a, b, c...)]
  *     [DROP|DETACH|ATTACH PARTITION|PART partition, ...]
  *     [FETCH PARTITION partition FROM ...]
  *     [FREEZE PARTITION]
  */
class ParserAlterQuery : public IParserBase
{
protected:
    const char * getName() const { return "ALTER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};

} // namespace DB
