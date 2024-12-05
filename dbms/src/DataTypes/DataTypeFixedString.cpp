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

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnUtil.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <IO/Buffer/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>


namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_READ_ALL_DATA;
extern const int TOO_LARGE_STRING_SIZE;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int UNEXPECTED_AST_STRUCTURE;
} // namespace ErrorCodes


std::string DataTypeFixedString::getName() const
{
    return "FixedString(" + toString(n) + ")";
}


void DataTypeFixedString::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const auto & s = get<const String &>(field);
    ostr.write(s.data(), std::min(s.size(), n));
    if (s.size() < n)
        for (size_t i = s.size(); i < n; ++i)
            ostr.write(0);
}


void DataTypeFixedString::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    field = String();
    auto & s = get<String &>(field);
    s.resize(n);
    istr.readStrict(&s[0], n);
}


void DataTypeFixedString::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    ostr.write(
        reinterpret_cast<const char *>(&static_cast<const ColumnFixedString &>(column).getChars()[n * row_num]),
        n);
}


void DataTypeFixedString::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    ColumnFixedString::Chars_t & data = static_cast<ColumnFixedString &>(column).getChars();
    size_t old_size = data.size();
    data.resize(old_size + n);
    try
    {
        istr.readStrict(reinterpret_cast<char *>(&data[old_size]), n);
    }
    catch (...)
    {
        data.resize_assume_reserved(old_size);
        throw;
    }
}


void DataTypeFixedString::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit)
    const
{
    const ColumnFixedString::Chars_t & data = typeid_cast<const ColumnFixedString &>(column).getChars();

    size_t size = data.size() / n;

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    ostr.write(reinterpret_cast<const char *>(&data[n * offset]), n * limit);
}


namespace
{
inline void FixedStringDeserializeBinaryBulkWithFilter(
    size_t n,
    ColumnFixedString::Chars_t & data,
    ReadBuffer & istr,
    size_t limit,
    const IColumn::Filter * filter)
{
    size_t current_size = data.size();
    data.resize(current_size + n * limit);

    if (!filter)
    {
        size_t size = istr.readBig(reinterpret_cast<char *>(&data[current_size]), n * limit);
        data.resize(current_size + size);
        return;
    }

    const UInt8 * filt_pos = filter->data();
    const UInt8 * filt_end = filt_pos + limit;
    const UInt8 * filt_end_aligned = filt_pos + limit / FILTER_SIMD_BYTES * FILTER_SIMD_BYTES;

    while (filt_pos < filt_end_aligned)
    {
        UInt64 mask = ToBits64(filt_pos);
        if likely (0 != mask)
        {
            if (const UInt8 prefix_to_copy = prefixToCopy(mask); 0xFF != prefix_to_copy)
            {
                size_t size = istr.read(reinterpret_cast<char *>(&data[current_size]), n * prefix_to_copy);
                current_size += size;
                istr.ignore(n * (FILTER_SIMD_BYTES - prefix_to_copy));
            }
            else
            {
                if (const UInt8 suffix_to_copy = suffixToCopy(mask); 0xFF != suffix_to_copy)
                {
                    istr.ignore(n * (FILTER_SIMD_BYTES - suffix_to_copy));
                    size_t size = istr.read(reinterpret_cast<char *>(&data[current_size]), n * suffix_to_copy);
                    current_size += size;
                }
                else
                {
                    size_t index = 0;
                    while (mask)
                    {
                        size_t m = std::countr_zero(mask);
                        istr.ignore(n * (m - index));
                        size_t size = istr.read(reinterpret_cast<char *>(&data[current_size]), n);
                        current_size += size;
                        index = m + 1;
                        mask &= mask - 1;
                    }
                    istr.ignore(n * (FILTER_SIMD_BYTES - index));
                }
            }
        }
        else
        {
            istr.ignore(n * FILTER_SIMD_BYTES);
        }

        filt_pos += FILTER_SIMD_BYTES;
    }

    while (filt_pos < filt_end)
    {
        if (*filt_pos)
        {
            size_t size = istr.read(reinterpret_cast<char *>(&data[current_size]), n);
            current_size += size;
        }
        else
        {
            istr.ignore(n);
        }

        ++filt_pos;
    }

    data.resize(current_size);
}
} // namespace


void DataTypeFixedString::deserializeBinaryBulk(
    IColumn & column,
    ReadBuffer & istr,
    size_t limit,
    double /*avg_value_size_hint*/,
    const IColumn::Filter * filter) const
{
    auto & data = typeid_cast<ColumnFixedString &>(column).getChars();
    FixedStringDeserializeBinaryBulkWithFilter(n, data, istr, limit, filter);
}


void DataTypeFixedString::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeString(
        reinterpret_cast<const char *>(&static_cast<const ColumnFixedString &>(column).getChars()[n * row_num]),
        n,
        ostr);
}


void DataTypeFixedString::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const char * pos
        = reinterpret_cast<const char *>(&static_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeAnyEscapedString<'\''>(pos, pos + n, ostr);
}


template <typename Reader>
static inline void read(const DataTypeFixedString & self, IColumn & column, Reader && reader)
{
    ColumnFixedString::Chars_t & data = typeid_cast<ColumnFixedString &>(column).getChars();
    size_t prev_size = data.size();

    try
    {
        reader(data);
    }
    catch (...)
    {
        data.resize_assume_reserved(prev_size);
        throw;
    }

    if (data.size() < prev_size + self.getN())
        data.resize_fill_zero(prev_size + self.getN());

    if (data.size() > prev_size + self.getN())
    {
        data.resize_assume_reserved(prev_size);
        throw Exception("Too large value for " + self.getName(), ErrorCodes::TOO_LARGE_STRING_SIZE);
    }
}


void DataTypeFixedString::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
    read(*this, column, [&istr](ColumnFixedString::Chars_t & data) { readEscapedStringInto(data, istr); });
}


void DataTypeFixedString::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const char * pos
        = reinterpret_cast<const char *>(&static_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeAnyQuotedString<'\''>(pos, pos + n, ostr);
}


void DataTypeFixedString::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
    read(*this, column, [&istr](ColumnFixedString::Chars_t & data) { readQuotedStringInto<true>(data, istr); });
}


void DataTypeFixedString::serializeTextJSON(
    const IColumn & column,
    size_t row_num,
    WriteBuffer & ostr,
    const FormatSettingsJSON &) const
{
    const char * pos
        = reinterpret_cast<const char *>(&static_cast<const ColumnFixedString &>(column).getChars()[n * row_num]);
    writeJSONString(pos, pos + n, ostr);
}


void DataTypeFixedString::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
{
    read(*this, column, [&istr](ColumnFixedString::Chars_t & data) { readJSONStringInto(data, istr); });
}


MutableColumnPtr DataTypeFixedString::createColumn() const
{
    return ColumnFixedString::create(n);
}


bool DataTypeFixedString::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && n == static_cast<const DataTypeFixedString &>(rhs).n;
}


static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception(
            "FixedString data type family must have exactly one argument - size in bytes",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * argument = typeid_cast<const ASTLiteral *>(arguments->children[0].get());
    if (!argument || argument->value.getType() != Field::Types::UInt64 || argument->value.get<UInt64>() == 0)
        throw Exception(
            "FixedString data type family must have a number (positive integer) as its argument",
            ErrorCodes::UNEXPECTED_AST_STRUCTURE);

    return std::make_shared<DataTypeFixedString>(argument->value.get<UInt64>());
}


void registerDataTypeFixedString(DataTypeFactory & factory)
{
    factory.registerDataType("FixedString", create);

    /// Compatibility alias.
    factory.registerDataType("BINARY", create, DataTypeFactory::CaseInsensitive);
}

} // namespace DB
