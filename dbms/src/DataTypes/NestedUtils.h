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

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>


namespace DB
{
namespace Nested
{
std::string concatenateName(const std::string & nested_table_name, const std::string & nested_field_name);

std::pair<std::string, std::string> splitName(const std::string & name);

/// Returns the prefix of the name to the first '.'. Or the name is unchanged if there is no dot.
std::string extractTableName(const std::string & nested_name);

/// Replace Array(Tuple(...)) columns to a multiple of Array columns in a form of `column_name.element_name`.
NamesAndTypesList flatten(const NamesAndTypesList & names_and_types);
Block flatten(const Block & block);

/// Collect Array columns in a form of `column_name.element_name` to single Array(Tuple(...)) column.
NamesAndTypesList collect(const NamesAndTypesList & names_and_types);
}; // namespace Nested

} // namespace DB
