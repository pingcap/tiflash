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

#include <DataTypes/IDataType.h>

namespace DB
{

/** Knows the names and types of all possible virtual columns.
  * It is necessary for engines that redirect a request to other tables without knowing in advance what virtual columns they contain.
  */
class VirtualColumnFactory
{
public:
    static bool hasColumn(const String & name);
    static DataTypePtr getType(const String & name);

    static DataTypePtr tryGetType(const String & name);
};

}
