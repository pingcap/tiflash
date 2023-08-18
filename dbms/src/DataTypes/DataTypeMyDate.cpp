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


#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMyDate.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{
void DataTypeMyDate::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeMyDateText(static_cast<const ColumnUInt64 &>(column).getData()[row_num], ostr);
}

static void deserializeText(IColumn & column, ReadBuffer & istr)
{
    UInt64 x = 0;
    readMyDateText(x, istr);
    static_cast<ColumnUInt64 &>(column).getData().push_back(x);
}

void DataTypeMyDate::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    serializeText(column, row_num, ostr);
}

void DataTypeMyDate::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
    deserializeText(column, istr);
}

void DataTypeMyDate::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr);
    writeChar('\'', ostr);
}

void DataTypeMyDate::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
    UInt64 x = 0;
    assertChar('\'', istr);
    readMyDateText(x, istr);
    assertChar('\'', istr);
    static_cast<ColumnUInt64 &>(column).getData().push_back(
        x); /// It's important to do this at the end - for exception safety.
}

void DataTypeMyDate::serializeTextJSON(
    const IColumn & column,
    size_t row_num,
    WriteBuffer & ostr,
    const FormatSettingsJSON &) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr);
    writeChar('"', ostr);
}

void DataTypeMyDate::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
{
    UInt64 x = 0;
    assertChar('"', istr);
    readMyDateText(x, istr);
    assertChar('"', istr);
    static_cast<ColumnUInt64 &>(column).getData().push_back(x);
}

void DataTypeMyDate::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr);
    writeChar('"', ostr);
}

void DataTypeMyDate::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char /*delimiter*/) const
{
    UInt64 value = 0;
    readCSV(value, istr);
    static_cast<ColumnUInt64 &>(column).getData().push_back(value);
}

bool DataTypeMyDate::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}


void registerDataTypeMyDate(DataTypeFactory & factory)
{
    factory.registerSimpleDataType(
        "MyDate",
        [] { return DataTypePtr(std::make_shared<DataTypeMyDate>()); },
        DataTypeFactory::CaseInsensitive);
}

} // namespace DB
