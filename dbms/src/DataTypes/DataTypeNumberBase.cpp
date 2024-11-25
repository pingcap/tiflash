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
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsCommon.h>
#include <Common/NaNUtils.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNumberBase.h>
#include <DataTypes/FormatSettingsJSON.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <type_traits>


namespace DB
{
template <typename T>
void DataTypeNumberBase<T>::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeText(static_cast<const ColumnVector<T> &>(column).getData()[row_num], ostr);
}

template <typename T>
void DataTypeNumberBase<T>::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    serializeText(column, row_num, ostr);
}


template <typename T>
static void deserializeText(IColumn & column, ReadBuffer & istr)
{
    T x{};

    if constexpr (std::is_integral_v<T> && std::is_arithmetic_v<T>)
        readIntTextUnsafe(x, istr);
    else
        readText(x, istr);

    static_cast<ColumnVector<T> &>(column).getData().push_back(x);
}


template <typename T>
void DataTypeNumberBase<T>::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
    deserializeText<T>(column, istr);
}

template <typename T>
void DataTypeNumberBase<T>::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    serializeText(column, row_num, ostr);
}

template <typename T>
void DataTypeNumberBase<T>::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
    deserializeText<T>(column, istr);
}


template <typename T>
static inline void writeDenormalNumber(T x, WriteBuffer & ostr)
{
    if constexpr (std::is_floating_point_v<T>)
    {
        if (std::signbit(x))
        {
            if (isNaN(x))
                writeCString("-nan", ostr);
            else
                writeCString("-inf", ostr);
        }
        else
        {
            if (isNaN(x))
                writeCString("nan", ostr);
            else
                writeCString("inf", ostr);
        }
    }
    else
    {
        /// This function is not called for non floating point numbers.
        (void)x;
    }
}


template <typename T>
void DataTypeNumberBase<T>::serializeTextJSON(
    const IColumn & column,
    size_t row_num,
    WriteBuffer & ostr,
    const FormatSettingsJSON & settings) const
{
    auto x = static_cast<const ColumnVector<T> &>(column).getData()[row_num];
    bool is_finite = isFinite(x);

    const bool need_quote = (std::is_integral_v<T> && (sizeof(T) == 8) && settings.force_quoting_64bit_integers)
        || (settings.output_format_json_quote_denormals && !is_finite);

    if (need_quote)
        writeChar('"', ostr);

    if (is_finite)
        writeText(x, ostr);
    else if (!settings.output_format_json_quote_denormals)
        writeCString("null", ostr);
    else
        writeDenormalNumber(x, ostr);

    if (need_quote)
        writeChar('"', ostr);
}

template <typename T>
void DataTypeNumberBase<T>::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
{
    bool has_quote = false;
    if (!istr.eof() && *istr.position() == '"') /// We understand the number both in quotes and without.
    {
        has_quote = true;
        ++istr.position();
    }

    FieldType x;

    /// null
    if (!has_quote && !istr.eof() && *istr.position() == 'n')
    {
        ++istr.position();
        assertString("ull", istr);

        x = NaNOrZero<T>();
    }
    else
    {
        static constexpr bool is_uint8 = std::is_same_v<T, UInt8>;
        static constexpr bool is_int8 = std::is_same_v<T, Int8>;

        if (is_uint8 || is_int8)
        {
            // extra conditions to parse true/false strings into 1/0
            if (istr.eof())
                throwReadAfterEOF();
            if (*istr.position() == 't' || *istr.position() == 'f')
            {
                bool tmp = false;
                readBoolTextWord(tmp, istr);
                x = tmp;
            }
            else
                readText(x, istr);
        }
        else
        {
            readText(x, istr);
        }

        if (has_quote)
            assertChar('"', istr);
    }

    static_cast<ColumnVector<T> &>(column).getData().push_back(x);
}

template <typename T>
Field DataTypeNumberBase<T>::getDefault() const
{
    return typename NearestFieldType<FieldType>::Type();
}

template <typename T>
void DataTypeNumberBase<T>::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    /// ColumnVector<T>::value_type is a narrower type. For example, UInt8, when the Field type is UInt64
    auto x = get<typename NearestFieldType<FieldType>::Type>(field);
    writeBinary(x, ostr);
}

template <typename T>
void DataTypeNumberBase<T>::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    typename ColumnVector<T>::value_type x;
    readBinary(x, istr);
    field = static_cast<typename NearestFieldType<FieldType>::Type>(x);
}

template <typename T>
void DataTypeNumberBase<T>::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeBinary(static_cast<const ColumnVector<T> &>(column).getData()[row_num], ostr);
}

template <typename T>
void DataTypeNumberBase<T>::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    typename ColumnVector<T>::value_type x;
    readBinary(x, istr);
    static_cast<ColumnVector<T> &>(column).getData().push_back(x);
}

template <typename T>
void DataTypeNumberBase<T>::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit)
    const
{
    const typename ColumnVector<T>::Container & x = typeid_cast<const ColumnVector<T> &>(column).getData();

    size_t size = x.size();

    if (limit == 0 || offset + limit > size)
        limit = size - offset;

    ostr.write(reinterpret_cast<const char *>(&x[offset]), sizeof(typename ColumnVector<T>::value_type) * limit);
}

template <typename T>
void DataTypeNumberBase<T>::deserializeBinaryBulk(
    IColumn & column,
    ReadBuffer & istr,
    size_t limit,
    double /*avg_value_size_hint*/,
    IColumn::Filter * filter) const
{
    auto & x = typeid_cast<ColumnVector<T> &>(column).getData();
    size_t current_size = x.size();
    constexpr auto field_size = sizeof(typename ColumnVector<T>::value_type);
    if (!filter)
    {
        x.resize(current_size + limit);
        size_t size = istr.readBig(reinterpret_cast<char *>(&x[current_size]), field_size * limit);
        x.resize(current_size + size / field_size);
        return;
    }

    const size_t passed = countBytesInFilter(filter->data(), limit);
    x.resize(current_size + passed);
    UInt8 prev = (*filter)[0];
    size_t count = 1;
    for (size_t i = 1; i < limit; ++i)
    {
        bool break_point = ((*filter)[i] != prev);
        if (break_point && prev)
        {
            size_t size = istr.read(reinterpret_cast<char *>(&x[current_size]), field_size * count);
            current_size += size / field_size;
            count = 1;
        }
        else if (break_point && !prev)
        {
            istr.ignore(field_size * count);
            prev = (*filter)[i];
            count = 1;
        }
        else
        {
            ++count;
        }
    }
    if (prev)
    {
        size_t size = istr.read(reinterpret_cast<char *>(&x[current_size]), field_size * count);
        current_size += size / field_size;
    }

    x.resize(current_size);
}

template <typename T>
MutableColumnPtr DataTypeNumberBase<T>::createColumn() const
{
    return ColumnVector<T>::create();
}

template <typename T>
bool DataTypeNumberBase<T>::isValueRepresentedByInteger() const
{
    return std::is_integral_v<T>;
}


/// Explicit template instantiations - to avoid code bloat in headers.
template class DataTypeNumberBase<UInt8>;
template class DataTypeNumberBase<UInt16>;
template class DataTypeNumberBase<UInt32>;
template class DataTypeNumberBase<UInt64>;
template class DataTypeNumberBase<UInt128>;
template class DataTypeNumberBase<Int8>;
template class DataTypeNumberBase<Int16>;
template class DataTypeNumberBase<Int32>;
template class DataTypeNumberBase<Int64>;
template class DataTypeNumberBase<Float32>;
template class DataTypeNumberBase<Float64>;

} // namespace DB
