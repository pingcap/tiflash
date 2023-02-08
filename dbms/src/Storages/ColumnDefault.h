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
#include <common/robin_hood.h>

#include <string>


namespace DB
{

enum class ColumnDefaultKind
{
    Default,
    Materialized,
    Alias
};


ColumnDefaultKind columnDefaultKindFromString(const std::string & str);
std::string toString(const ColumnDefaultKind kind);


struct ColumnDefault
{
    ColumnDefaultKind kind;
    ASTPtr expression;
};


bool operator==(const ColumnDefault & lhs, const ColumnDefault & rhs);


using ColumnDefaults = robin_hood::unordered_map<std::string, ColumnDefault>;


} // namespace DB
