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

#include <Parsers/queryToString.h>
#include <Storages/ColumnDefault.h>


namespace DB
{


ColumnDefaultKind columnDefaultKindFromString(const std::string & str)
{
    static const std::unordered_map<std::string, ColumnDefaultKind> map{
        {"DEFAULT", ColumnDefaultKind::Default},
        {"MATERIALIZED", ColumnDefaultKind::Materialized},
        {"ALIAS", ColumnDefaultKind::Alias}};

    const auto it = map.find(str);
    return it != std::end(map) ? it->second : throw Exception{"Unknown column default specifier: " + str};
}


std::string toString(const ColumnDefaultKind kind)
{
    static const std::unordered_map<ColumnDefaultKind, std::string> map{
        {ColumnDefaultKind::Default, "DEFAULT"},
        {ColumnDefaultKind::Materialized, "MATERIALIZED"},
        {ColumnDefaultKind::Alias, "ALIAS"}};

    const auto it = map.find(kind);
    return it != std::end(map) ? it->second : throw Exception{"Invalid ColumnDefaultKind"};
}


bool operator==(const ColumnDefault & lhs, const ColumnDefault & rhs)
{
    return lhs.kind == rhs.kind && queryToString(lhs.expression) == queryToString(rhs.expression);
}

} // namespace DB
