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

#include <IO/WriteHelpers.h>
#include <Parsers/ASTSubquery.h>

namespace DB
{

String ASTSubquery::getColumnNameImpl() const
{
    /// This is a hack. We use alias, if available, because otherwise tree could change during analysis.
    if (!alias.empty())
        return alias;

    Hash hash = getTreeHash();
    return "__subquery_" + toString(hash.first) + "_" + toString(hash.second);
}

} // namespace DB
