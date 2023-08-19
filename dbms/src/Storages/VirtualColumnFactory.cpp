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

#include <Storages/VirtualColumnFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}


DataTypePtr VirtualColumnFactory::getType(const String & name)
{
    auto res = tryGetType(name);
    if (!res)
        throw Exception("There is no column " + name + " in table.", ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
    return res;
}

bool VirtualColumnFactory::hasColumn(const String & name)
{
    return !!tryGetType(name);
}

DataTypePtr VirtualColumnFactory::tryGetType(const String & name)
{
    if (name == "_table")         return std::make_shared<DataTypeString>();
    if (name == "_part")          return std::make_shared<DataTypeString>();
    if (name == "_part_index")    return std::make_shared<DataTypeUInt64>();
    if (name == "_sample_factor") return std::make_shared<DataTypeFloat64>();
    return nullptr;
}

}
