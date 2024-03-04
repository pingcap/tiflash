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

#include <DataTypes/IDataType.h>

namespace DB
{
namespace CodecUtils
{
struct DataTypeWithTypeName
{
    DataTypeWithTypeName(const DataTypePtr & t, const String & n)
        : type(t)
        , name(n)
    {
    }

    DataTypePtr type;
    String name;
};

void checkColumnSize(const String & identifier, size_t expected, size_t actual);
void checkDataTypeName(const String & identifier, size_t column_index, const String & expected, const String & actual);
} // namespace CodecUtils
} // namespace DB
