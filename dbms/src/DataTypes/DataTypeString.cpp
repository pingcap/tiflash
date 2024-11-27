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
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>

#if __SSE2__
#include <emmintrin.h>
#endif


namespace DB
{
void DataTypeString::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const auto & s = get<const String &>(field);
    writeVarUInt(s.size(), ostr);
    writeString(s, ostr);
}


void DataTypeString::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    UInt64 size;
    readVarUInt(size, istr);
    field = String();
    auto & s = get<String &>(field);
    s.resize(size);
    istr.readStrict(&s[0], size);
}


void DataTypeString::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const StringRef & s = static_cast<const ColumnString &>(column).getDataAt(row_num);
    writeVarUInt(s.size, ostr);
    writeString(s, ostr);
}


void DataTypeString::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    auto & column_string = static_cast<ColumnString &>(column);
    ColumnString::Chars_t & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();

    UInt64 size;
    readVarUInt(size, istr);

    size_t old_chars_size = data.size();
    size_t offset = old_chars_size + size + 1;
    offsets.push_back(offset);

    try
    {
        data.resize(offset);
        istr.readStrict(reinterpret_cast<char *>(&data[offset - size - 1]), size);
        data.back() = 0;
    }
    catch (...)
    {
        offsets.pop_back();
        data.resize_assume_reserved(old_chars_size);
        throw;
    }
}


void DataTypeString::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const ColumnString & column_string = typeid_cast<const ColumnString &>(column);
    const ColumnString::Chars_t & data = column_string.getChars();
    const ColumnString::Offsets & offsets = column_string.getOffsets();

    size_t size = column_string.size();
    if (!size)
        return;

    size_t end = limit && offset + limit < size ? offset + limit : size;

    if (offset == 0)
    {
        UInt64 str_size = offsets[0] - 1;
        writeVarUInt(str_size, ostr);
        ostr.write(reinterpret_cast<const char *>(data.data()), str_size);

        ++offset;
    }

    for (size_t i = offset; i < end; ++i)
    {
        UInt64 str_size = offsets[i] - offsets[i - 1] - 1;
        writeVarUInt(str_size, ostr);
        ostr.write(reinterpret_cast<const char *>(&data[offsets[i - 1]]), str_size);
    }
}


template <int UNROLL_TIMES>
static NO_INLINE void deserializeBinarySSE2(
    ColumnString::Chars_t & data,
    ColumnString::Offsets & offsets,
    ReadBuffer & istr,
    size_t limit)
{
    size_t offset = data.size();
    for (size_t i = 0; i < limit; ++i)
    {
        if (istr.eof())
            break;

        UInt64 size;
        readVarUInt(size, istr);

        offset += size + 1;
        offsets.push_back(offset);

        data.resize(offset);

        if (size)
        {
#ifdef __SSE2__
            /// An optimistic branch in which more efficient copying is possible.
            if (offset + 16 * UNROLL_TIMES <= data.capacity()
                && istr.position() + size + 16 * UNROLL_TIMES <= istr.buffer().end())
            {
                const auto * sse_src_pos = reinterpret_cast<const __m128i *>(istr.position());
                const __m128i * sse_src_end
                    = sse_src_pos + (size + (16 * UNROLL_TIMES - 1)) / 16 / UNROLL_TIMES * UNROLL_TIMES;
                auto * sse_dst_pos = reinterpret_cast<__m128i *>(&data[offset - size - 1]);

                while (sse_src_pos < sse_src_end)
                {
                    for (size_t j = 0; j < UNROLL_TIMES; ++j)
                        _mm_storeu_si128(sse_dst_pos + j, _mm_loadu_si128(sse_src_pos + j));

                    sse_src_pos += UNROLL_TIMES;
                    sse_dst_pos += UNROLL_TIMES;
                }

                istr.position() += size;
            }
            else
#endif
            {
                istr.readStrict(reinterpret_cast<char *>(&data[offset - size - 1]), size);
            }
        }

        data[offset - 1] = 0;
    }
}


void DataTypeString::deserializeBinaryBulk(
    IColumn & column,
    ReadBuffer & istr,
    size_t limit,
    double avg_value_size_hint) const
{
    ColumnString & column_string = typeid_cast<ColumnString &>(column);
    ColumnString::Chars_t & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();

    double avg_chars_size = 1; /// By default reserve only for empty strings.

    if (avg_value_size_hint > 0.0 && avg_value_size_hint > sizeof(offsets[0]))
    {
        /// Randomly selected.
        constexpr auto avg_value_size_hint_reserve_multiplier = 1.2;

        avg_chars_size = (avg_value_size_hint - sizeof(offsets[0])) * avg_value_size_hint_reserve_multiplier;
    }

    size_t size_to_reserve = data.size() + static_cast<size_t>(std::ceil(limit * avg_chars_size));
    data.reserve(size_to_reserve);

    offsets.reserve(offsets.size() + limit);

    if (avg_chars_size >= 64)
        deserializeBinarySSE2<4>(data, offsets, istr, limit);
    else if (avg_chars_size >= 48)
        deserializeBinarySSE2<3>(data, offsets, istr, limit);
    else if (avg_chars_size >= 32)
        deserializeBinarySSE2<2>(data, offsets, istr, limit);
    else
        deserializeBinarySSE2<1>(data, offsets, istr, limit);
}


void DataTypeString::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeString(static_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


void DataTypeString::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeEscapedString(static_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


template <typename Reader>
static inline void read(IColumn & column, Reader && reader)
{
    auto & column_string = static_cast<ColumnString &>(column);
    ColumnString::Chars_t & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();

    size_t old_chars_size = data.size();
    size_t old_offsets_size = offsets.size();

    try
    {
        reader(data);
        data.push_back(0);
        offsets.push_back(data.size());
    }
    catch (...)
    {
        offsets.resize_assume_reserved(old_offsets_size);
        data.resize_assume_reserved(old_chars_size);
        throw;
    }
}


void DataTypeString::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
    read(column, [&](ColumnString::Chars_t & data) { readEscapedStringInto(data, istr); });
}


void DataTypeString::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeQuotedString(static_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


void DataTypeString::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
    read(column, [&](ColumnString::Chars_t & data) { readQuotedStringInto<true>(data, istr); });
}


void DataTypeString::serializeTextJSON(
    const IColumn & column,
    size_t row_num,
    WriteBuffer & ostr,
    const FormatSettingsJSON &) const
{
    writeJSONString(static_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


void DataTypeString::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
{
    read(column, [&](ColumnString::Chars_t & data) { readJSONStringInto(data, istr); });
}


MutableColumnPtr DataTypeString::createColumn() const
{
    return ColumnString::create();
}


bool DataTypeString::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}


void registerDataTypeString(DataTypeFactory & factory)
{
    auto creator = static_cast<DataTypePtr (*)()>([] { return DataTypePtr(std::make_shared<DataTypeString>()); });

    factory.registerSimpleDataType("String", creator);

    /// These synonims are added for compatibility.

    factory.registerSimpleDataType("CHAR", creator, DataTypeFactory::CaseInsensitive);
    factory.registerSimpleDataType("VARCHAR", creator, DataTypeFactory::CaseInsensitive);
    factory.registerSimpleDataType("TEXT", creator, DataTypeFactory::CaseInsensitive);
    factory.registerSimpleDataType("TINYTEXT", creator, DataTypeFactory::CaseInsensitive);
    factory.registerSimpleDataType("MEDIUMTEXT", creator, DataTypeFactory::CaseInsensitive);
    factory.registerSimpleDataType("LONGTEXT", creator, DataTypeFactory::CaseInsensitive);
    factory.registerSimpleDataType("BLOB", creator, DataTypeFactory::CaseInsensitive);
    factory.registerSimpleDataType("TINYBLOB", creator, DataTypeFactory::CaseInsensitive);
    factory.registerSimpleDataType("MEDIUMBLOB", creator, DataTypeFactory::CaseInsensitive);
    factory.registerSimpleDataType("LONGBLOB", creator, DataTypeFactory::CaseInsensitive);
}

namespace
{

using Offset = ColumnString::Offset;

// Returns <offsets_stream, chars_stream>.
template <typename B, typename G>
std::pair<B *, B *> getStream(const G & getter, IDataType::SubstreamPath & path)
{
    auto * chars_stream = getter(path);
    path.emplace_back(IDataType::Substream::StringSizes);
    auto * offsets_stream = getter(path);
    return {offsets_stream, chars_stream};
}

PaddedPODArray<Offset> offsetToStrSize(
    const ColumnString::Offsets & chars_offsets,
    const size_t begin,
    const size_t end)
{
    assert(!chars_offsets.empty());
    // The class PODArrayBase ensure chars_offsets[-1] is well defined as 0.
    // For details, check the `pad_left` argument in PODArrayBase.
    // In the for loop code below, when `begin` and `i` are 0:
    // str_sizes[0] = chars_offsets[0] - chars_offsets[-1];
    assert(chars_offsets[-1] == 0);

    PaddedPODArray<Offset> str_sizes(end - begin);
    auto chars_offsets_pos = chars_offsets.begin() + begin;

    // clang-format off
    #pragma clang loop vectorize(enable)
    // clang-format on
    for (ssize_t i = 0; i < static_cast<ssize_t>(str_sizes.size()); ++i)
    {
        str_sizes[i] = chars_offsets_pos[i] - chars_offsets_pos[i - 1];
    }
    return str_sizes;
}

void strSizeToOffset(const PaddedPODArray<Offset> & str_sizes, ColumnString::Offsets & chars_offsets)
{
    assert(!str_sizes.empty());
    const auto initial_size = chars_offsets.size();
    chars_offsets.resize(initial_size + str_sizes.size());
    size_t i = 0;
    if (initial_size == 0)
    {
        chars_offsets[i] = str_sizes[0];
        ++i;
    }
    assert(initial_size + i > 0);
    // Cannot be vectorize by compiler because chars_offsets[i] depends on chars_offsets[i-1]
    // #pragma clang loop vectorize(enable)
    for (; i < str_sizes.size(); ++i)
    {
        chars_offsets[i + initial_size] = str_sizes[i] + chars_offsets[i + initial_size - 1];
    }
}

std::pair<size_t, size_t> serializeOffsetsBinary(
    const ColumnString::Offsets & chars_offsets,
    WriteBuffer & ostr,
    size_t offset,
    size_t limit)
{
    // [begin, end) is the range that need to be serialized of `chars_offsets`.
    const auto begin = offset;
    const auto end = limit != 0 && offset + limit < chars_offsets.size() ? offset + limit : chars_offsets.size();

    PaddedPODArray<Offset> sizes = offsetToStrSize(chars_offsets, begin, end);
    ostr.write(reinterpret_cast<const char *>(sizes.data()), sizeof(Offset) * sizes.size());

    // [chars_begin, chars_end) is the range that need to be serialized of `chars`.
    const auto chars_begin = begin == 0 ? 0 : chars_offsets[begin - 1];
    const auto chars_end = chars_offsets[end - 1];
    return {chars_begin, chars_end};
}

void serializeCharsBinary(const ColumnString::Chars_t & chars, WriteBuffer & ostr, size_t begin, size_t end)
{
    ostr.write(reinterpret_cast<const char *>(&chars[begin]), end - begin);
}

size_t deserializeOffsetsBinary(ColumnString::Offsets & chars_offsets, ReadBuffer & istr, size_t limit)
{
    PaddedPODArray<Offset> str_sizes(limit);
    const auto size = istr.readBig(reinterpret_cast<char *>(str_sizes.data()), sizeof(Offset) * limit);
    str_sizes.resize(size / sizeof(Offset));
    strSizeToOffset(str_sizes, chars_offsets);
    return std::accumulate(str_sizes.begin(), str_sizes.end(), 0uz);
}

void deserializeCharsBinary(ColumnString::Chars_t & chars, ReadBuffer & istr, size_t bytes)
{
    const auto initial_size = chars.size();
    chars.resize(initial_size + bytes);
    istr.readStrict(reinterpret_cast<char *>(&chars[initial_size]), bytes);
}

void serializeBinaryBulkV2(
    const IColumn & column,
    WriteBuffer & offsets_stream,
    WriteBuffer & chars_stream,
    size_t offset,
    size_t limit)
{
    if (column.empty())
        return;
    const auto & column_string = typeid_cast<const ColumnString &>(column);
    const auto & chars = column_string.getChars();
    const auto & offsets = column_string.getOffsets();
    auto [chars_begin, chars_end] = serializeOffsetsBinary(offsets, offsets_stream, offset, limit);
    serializeCharsBinary(chars, chars_stream, chars_begin, chars_end);
}

void deserializeBinaryBulkV2(IColumn & column, ReadBuffer & offsets_stream, ReadBuffer & chars_stream, size_t limit)
{
    if (limit == 0)
        return;
    auto & column_string = typeid_cast<ColumnString &>(column);
    auto & chars = column_string.getChars();
    auto & offsets = column_string.getOffsets();
    auto bytes = deserializeOffsetsBinary(offsets, offsets_stream, limit);
    deserializeCharsBinary(chars, chars_stream, bytes);
}

} // namespace

void DataTypeString::enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const
{
    callback(path);
    if (serdes_fmt == SerdesFormat::SeparateSizeAndChars)
    {
        path.emplace_back(Substream::StringSizes);
        callback(path);
    }
}

void DataTypeString::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    const OutputStreamGetter & getter,
    size_t offset,
    size_t limit,
    bool /*position_independent_encoding*/,
    SubstreamPath & path) const
{
    if (serdes_fmt == SerdesFormat::SeparateSizeAndChars)
    {
        auto [offsets_stream, chars_stream] = getStream<WriteBuffer, IDataType::OutputStreamGetter>(getter, path);
        serializeBinaryBulkV2(column, *offsets_stream, *chars_stream, offset, limit);
    }
    else
    {
        serializeBinaryBulk(column, *getter(path), offset, limit);
    }
}

void DataTypeString::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    const InputStreamGetter & getter,
    size_t limit,
    double avg_value_size_hint,
    bool /*position_independent_encoding*/,
    SubstreamPath & path) const
{
    if (serdes_fmt == SerdesFormat::SeparateSizeAndChars)
    {
        auto [offsets_stream, chars_stream] = getStream<ReadBuffer, IDataType::InputStreamGetter>(getter, path);
        deserializeBinaryBulkV2(column, *offsets_stream, *chars_stream, limit);
    }
    else
    {
        deserializeBinaryBulk(column, *getter(path), limit, avg_value_size_hint);
    }
}


} // namespace DB
