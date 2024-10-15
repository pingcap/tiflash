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
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <common/DateLUT.h>


namespace DB
{
DataTypeDateTime::DataTypeDateTime(const std::string & time_zone_name)
    : has_explicit_time_zone(!time_zone_name.empty())
    , time_zone(DateLUT::instance(time_zone_name))
{}

std::string DataTypeDateTime::getName() const
{
    if (!has_explicit_time_zone)
        return "DateTime";

    WriteBufferFromOwnString out;
    out << "DateTime(" << quote << time_zone.getTimeZone() << ")";
    return out.str();
}

void DataTypeDateTime::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeDateTimeText(static_cast<const ColumnUInt32 &>(column).getData()[row_num], ostr, time_zone);
}

void DataTypeDateTime::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    serializeText(column, row_num, ostr);
}

void DataTypeDateTime::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
    time_t x;
    readDateTimeText(x, istr, time_zone);
    static_cast<ColumnUInt32 &>(column).getData().push_back(x);
}

void DataTypeDateTime::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeChar('\'', ostr);
    serializeText(column, row_num, ostr);
    writeChar('\'', ostr);
}

void DataTypeDateTime::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
    time_t x;
    if (checkChar('\'', istr)) /// Cases: '2017-08-31 18:36:48' or '1504193808'
    {
        readDateTimeText(x, istr, time_zone);
        assertChar('\'', istr);
    }
    else /// Just 1504193808 or 01504193808
    {
        readIntText(x, istr);
    }
    static_cast<ColumnUInt32 &>(column).getData().push_back(
        x); /// It's important to do this at the end - for exception safety.
}

void DataTypeDateTime::serializeTextJSON(
    const IColumn & column,
    size_t row_num,
    WriteBuffer & ostr,
    const FormatSettingsJSON &) const
{
    writeChar('"', ostr);
    serializeText(column, row_num, ostr);
    writeChar('"', ostr);
}

void DataTypeDateTime::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
{
    time_t x;
    if (checkChar('"', istr))
    {
        readDateTimeText(x, istr, time_zone);
        assertChar('"', istr);
    }
    else
    {
        readIntText(x, istr);
    }
    static_cast<ColumnUInt32 &>(column).getData().push_back(x);
}

bool DataTypeDateTime::equals(const IDataType & rhs) const
{
    /// DateTime with different timezones are equal, because:
    /// "all types with different time zones are equivalent and may be used interchangingly."
    return typeid(rhs) == typeid(*this);
}


namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
} // namespace ErrorCodes

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments)
        return std::make_shared<DataTypeDateTime>();

    if (arguments->children.size() != 1)
        throw Exception(
            "DateTime data type can optionally have only one argument - time zone name",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * arg = typeid_cast<const ASTLiteral *>(arguments->children[0].get());
    if (!arg || arg->value.getType() != Field::Types::String)
        throw Exception(
            "Parameter for DateTime data type must be string literal",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<DataTypeDateTime>(arg->value.get<String>());
}

void registerDataTypeDateTime(DataTypeFactory & factory)
{
    factory.registerDataType("DateTime", create, DataTypeFactory::CaseInsensitive);
}


} // namespace DB
