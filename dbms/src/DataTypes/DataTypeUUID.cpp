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

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeUUID.h>


namespace DB
{
void DataTypeUUID::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeText(UUID(static_cast<const ColumnUInt128 &>(column).getData()[row_num]), ostr);
}

static void deserializeText(IColumn & column, ReadBuffer & istr)
{
    UUID x;
    readText(x, istr);
    static_cast<ColumnUInt128 &>(column).getData().push_back(x);
}

void DataTypeUUID::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    serializeText(column, row_num, ostr);
}

void DataTypeUUID::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
    deserializeText(column, istr);
}

void DataTypeUUID::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr);
    writeChar('\'', ostr);
}

void DataTypeUUID::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
    UUID x;
    assertChar('\'', istr);
    readText(x, istr);
    assertChar('\'', istr);
    static_cast<ColumnUInt128 &>(column).getData().push_back(
        x); /// It's important to do this at the end - for exception safety.
}

void DataTypeUUID::serializeTextJSON(
    const IColumn & column,
    size_t row_num,
    WriteBuffer & ostr,
    const FormatSettingsJSON &) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr);
    writeChar('"', ostr);
}

void DataTypeUUID::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
{
    UUID x;
    assertChar('"', istr);
    readText(x, istr);
    assertChar('"', istr);
    static_cast<ColumnUInt128 &>(column).getData().push_back(x);
}

bool DataTypeUUID::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}


void registerDataTypeUUID(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("UUID", [] { return DataTypePtr(std::make_shared<DataTypeUUID>()); });
}

} // namespace DB
