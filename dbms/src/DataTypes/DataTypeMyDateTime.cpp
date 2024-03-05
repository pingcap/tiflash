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
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <common/DateLUT.h>


namespace DB
{
DataTypeMyDateTime::DataTypeMyDateTime(int fraction_)
{
    fraction = fraction_;
    if (fraction < 0 || fraction > 6)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "fraction must >= 0 and <= 6, fraction={}", fraction_);
}

void DataTypeMyDateTime::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeMyDateTimeText(static_cast<const ColumnUInt64 &>(column).getData()[row_num], fraction, ostr);
}

void DataTypeMyDateTime::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    serializeText(column, row_num, ostr);
}

void DataTypeMyDateTime::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
    UInt64 x;
    readMyDateTimeText(x, fraction, istr);
    static_cast<ColumnUInt64 &>(column).getData().push_back(x);
}

void DataTypeMyDateTime::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr);
    writeChar('\'', ostr);
}

void DataTypeMyDateTime::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
    UInt64 x;
    if (checkChar('\'', istr)) /// Cases: '2017-08-31 18:36:48' or '1504193808'
    {
        readMyDateTimeText(x, fraction, istr);
        assertChar('\'', istr);
    }
    else /// Just 1504193808 or 01504193808
    {
        readIntText(x, istr);
    }
    static_cast<ColumnUInt64 &>(column).getData().push_back(
        x); /// It's important to do this at the end - for exception safety.
}

void DataTypeMyDateTime::serializeTextJSON(
    const IColumn & column,
    size_t row_num,
    WriteBuffer & ostr,
    const FormatSettingsJSON &) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr);
    writeChar('"', ostr);
}

void DataTypeMyDateTime::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
{
    UInt64 x;
    if (checkChar('"', istr))
    {
        readMyDateTimeText(x, fraction, istr);
        assertChar('"', istr);
    }
    else
    {
        readIntText(x, istr);
    }
    static_cast<ColumnUInt64 &>(column).getData().push_back(x);
}

void DataTypeMyDateTime::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr);
    writeChar('"', ostr);
}

void DataTypeMyDateTime::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char /*delimiter*/) const
{
    UInt64 x;
    readMyDateTimeCSV(x, fraction, istr);
    static_cast<ColumnUInt64 &>(column).getData().push_back(x);
}

bool DataTypeMyDateTime::equals(const IDataType & rhs) const
{
    /// DateTime with different timezones are equal, because:
    /// "all types with different time zones are equivalent and may be used interchangingly."
    return typeid(rhs) == typeid(*this)
        && getFraction() == dynamic_cast<const DataTypeMyDateTime *>(&rhs)->getFraction();
}


namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
} // namespace ErrorCodes

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments)
        return std::make_shared<DataTypeMyDateTime>(0);

    if (arguments->children.size() != 1)
        throw Exception(
            "MyDateTime data type can optionally have only one argument - fractional",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * arg = typeid_cast<const ASTLiteral *>(arguments->children[0].get());
    if (!arg || arg->value.getType() != Field::Types::UInt64)
        throw Exception(
            "Parameter for MyDateTime data type must be uint literal",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<DataTypeMyDateTime>(arg->value.get<int>());
}

void registerDataTypeMyDateTime(DataTypeFactory & factory)
{
    factory.registerDataType("MyDateTime", create, DataTypeFactory::CaseInsensitive);
}

} // namespace DB
