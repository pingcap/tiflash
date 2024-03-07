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

#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>


namespace DB
{
ColumnWithTypeAndName ColumnWithTypeAndName::cloneEmpty() const
{
    ColumnWithTypeAndName
        res{(column != nullptr ? column->cloneEmpty() : nullptr), type, name, column_id, default_value};
    return res;
}


bool ColumnWithTypeAndName::operator==(const ColumnWithTypeAndName & other) const
{
    // TODO should we check column_id here?
    return name == other.name && ((!type && !other.type) || (type && other.type && type->equals(*other.type)))
        && ((!column && !other.column) || (column && other.column && column->getName() == other.column->getName()));
}


void ColumnWithTypeAndName::dumpStructure(WriteBuffer & out) const
{
    out << name << ' ' << column_id;

    if (type)
        out << ' ' << type->getName();
    else
        out << " nullptr";

    if (column)
        out << ' ' << column->dumpStructure();
    else
        out << " nullptr";
}

String ColumnWithTypeAndName::dumpStructure() const
{
    WriteBufferFromOwnString out;
    dumpStructure(out);
    return out.releaseStr();
}

void ColumnWithTypeAndName::dumpJsonStructure(WriteBuffer & out) const
{
    out << fmt::format(
        R"json({{"name":"{}","id":{},"type":{},"column":{}}})json",
        name,
        column_id,
        (type ? "\"" + type->getName() + "\"" : "null"),
        (column ? "\"" + column->dumpStructure() + "\"" : "null"));
}

String ColumnWithTypeAndName::dumpJsonStructure() const
{
    WriteBufferFromOwnString out;
    dumpJsonStructure(out);
    return out.releaseStr();
}

} // namespace DB
