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

#include <DataTypes/DataTypeFunction.h>


namespace DB
{
std::string DataTypeFunction::getName() const
{
    std::string res = "Function(";
    if (argument_types.size() > 1)
        res += "(";
    for (size_t i = 0; i < argument_types.size(); ++i)
    {
        if (i > 0)
            res += ", ";
        const DataTypePtr & type = argument_types[i];
        res += type ? type->getName() : "?";
    }
    if (argument_types.size() > 1)
        res += ")";
    res += " -> ";
    res += return_type ? return_type->getName() : "?";
    res += ")";
    return res;
}

bool DataTypeFunction::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && getName() == rhs.getName();
}

} // namespace DB
