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
#include <lz4.h>
#include <streamvbyte.h>


namespace DB
{
void DataTypeString::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const String & s = get<const String &>(field);
    writeVarUInt(s.size(), ostr);
    writeString(s, ostr);
}


void DataTypeString::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    UInt64 size;
    readVarUInt(size, istr);
    field = String();
    String & s = get<String &>(field);
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
    ColumnString & column_string = static_cast<ColumnString &>(column);
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

double DataTypeString::serializeBinaryBulkWithCompression(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const ColumnString & column_string = typeid_cast<const ColumnString &>(column);
    const ColumnString::Chars_t & data = column_string.getChars();
    const ColumnString::Offsets & offsets = column_string.getOffsets();

    size_t size = column.size();
    if (!size)
        return 0;

    if (offset == 0)
    {
        if (limit == 0 || offset + limit > size)
            limit = size - offset;
#if 0
        auto raw_size = sizeof(ColumnString::Offset) * limit + offsets[limit - 1] + 3;
        auto need_size = streamvbyte_max_compressedbytes(raw_size / 4 + 18);
        ostr.forceNext(need_size);
        /// compress offsets
        size_t n32_off = sizeof(ColumnString::Offset) * limit / 4.0;
        size_t compsize_off = streamvbyte_encode(reinterpret_cast<const uint32_t *>(reinterpret_cast<const char *>(&offsets[offset])), n32_off, reinterpret_cast<uint8_t *>(ostr.position() + sizeof(size_t))); // encoding
        *(size_t *)ostr.position() = compsize_off;
        ostr.position() += compsize_off + sizeof(size_t);
        /// compress data
        size_t n32_data = (offsets[limit - 1] + 3) / 4;
        size_t compsize_data = streamvbyte_encode(reinterpret_cast<const uint32_t *>(reinterpret_cast<const char *>(&data[offset])), n32_data, reinterpret_cast<uint8_t *>(ostr.position() + sizeof(size_t))); // encoding
            /// write compression size of data
        // if(compsize_data >= offsets[limit-1])
        //     throw Exception("compression size is bigger for encoding string");
        *(size_t *)ostr.position() = compsize_data;
        ostr.position() += compsize_data + sizeof(size_t);
        return compsize_off / (n32_off * 4) * 10000 + compsize_data / offsets[limit - 1];
#else
        auto need_size = sizeof(ColumnString::Offset) * limit + LZ4_COMPRESSBOUND(offsets[limit - 1]) + 4 * sizeof(size_t);
        ostr.forceNext(need_size);
        /// compress offsets
        size_t n32_off = sizeof(ColumnString::Offset) * limit / 4.0;
        size_t compsize_off = streamvbyte_encode(reinterpret_cast<const uint32_t *>(reinterpret_cast<const char *>(&offsets[offset])), n32_off, reinterpret_cast<uint8_t *>(ostr.position() + sizeof(size_t))); // encoding
        *(size_t *)ostr.position() = compsize_off;
        ostr.position() += compsize_off + sizeof(size_t);
        /// compress data
        auto src_size = offsets[limit - 1];
        size_t compsize_data = LZ4_compress_default(reinterpret_cast<const char *>(&data[offset]), reinterpret_cast<char *>(ostr.position() + sizeof(size_t)), src_size, LZ4_COMPRESSBOUND(src_size));
        *(size_t *)ostr.position() = compsize_data;
        ostr.position() += compsize_data + sizeof(size_t);
        return compsize_off / (n32_off * 4.0) * 10000 + compsize_data * 1.0 / offsets[limit - 1];
#endif
    }
    else
    {
        throw Exception("string compression does not start from 0!");
    }
}
void DataTypeString::deserializeBinaryBulkWithCompression(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint [[maybe_unused]]) const
{
    ColumnString & column_string = typeid_cast<ColumnString &>(column);
    ColumnString::Chars_t & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();
#if 0
    /// decompress offsets
    size_t initial_size = offsets.size();
    assert(initial_size == 0);
    offsets.resize(initial_size + limit);
    size_t compsize = *(size_t *)istr.position();
    size_t n32_off = limit * sizeof(ColumnString::Offset) / 4.0;
    size_t compsize2 = streamvbyte_decode(reinterpret_cast<const uint8_t *>(istr.position() + sizeof(size_t)), reinterpret_cast<uint32_t *>(reinterpret_cast<char *>(&offsets[initial_size])), n32_off); // decoding (fast)
    offsets.resize(initial_size + limit);
    if (compsize != compsize2)
    {
        throw Exception("exchange compression size is not equal in DataTypeString->offsets");
    }
    istr.position() += compsize + sizeof(size_t);
    /// decompress data
    compsize = *(size_t *)istr.position();
    size_t n32_data = (offsets[limit - 1] + 3) / 4.0;
    data.resize(n32_data * 4);
    compsize2 = streamvbyte_decode(reinterpret_cast<const uint8_t *>(istr.position() + sizeof(size_t)), reinterpret_cast<uint32_t *>(reinterpret_cast<char *>(&data[initial_size])), n32_data); // decoding (fast)
    if (compsize != compsize2)
    {
        throw Exception("exchange compression size is not equal in DataTypeString->data");
    }
    istr.position() += compsize + sizeof(size_t);
    data.resize(offsets[limit - 1]);
#else
    /// decompress offsets
    size_t initial_size = offsets.size();
    assert(initial_size == 0);
    offsets.resize(initial_size + limit);
    size_t compsize = *(size_t *)istr.position();
    size_t n32_off = limit * sizeof(ColumnString::Offset) / 4.0;
    size_t compsize2 = streamvbyte_decode(reinterpret_cast<const uint8_t *>(istr.position() + sizeof(size_t)), reinterpret_cast<uint32_t *>(reinterpret_cast<char *>(&offsets[initial_size])), n32_off); // decoding (fast)
    offsets.resize(initial_size + limit);
    if (compsize != compsize2)
    {
        throw Exception("exchange compression size is not equal in DataTypeString->offsets");
    }
    istr.position() += compsize + sizeof(size_t);
    /// decompress data
    compsize = *(size_t *)istr.position();
    size_t src_size = offsets[limit - 1];
    data.resize(src_size);
    compsize2 = LZ4_decompress_fast(reinterpret_cast<const char *>(istr.position() + sizeof(size_t)), reinterpret_cast<char *>(&data[initial_size]), src_size);
    if (compsize != compsize2)
    {
        throw Exception("exchange compression size is not equal in DataTypeString->data");
    }
    istr.position() += compsize + sizeof(size_t);
    data.resize(offsets[limit - 1]);
#endif
}

void DataTypeString::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const ColumnString & column_string = typeid_cast<const ColumnString &>(column);
    const ColumnString::Chars_t & data = column_string.getChars();
    const ColumnString::Offsets & offsets = column_string.getOffsets();

    size_t size = column.size();
    if (!size)
        return;

    size_t end = limit && offset + limit < size
        ? offset + limit
        : size;

    if (offset == 0)
    {
        UInt64 str_size = offsets[0] - 1;
        writeVarUInt(str_size, ostr);
        ostr.write(reinterpret_cast<const char *>(&data[0]), str_size);

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
static NO_INLINE void deserializeBinarySSE2(ColumnString::Chars_t & data, ColumnString::Offsets & offsets, ReadBuffer & istr, size_t limit)
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
#if __SSE2__
            /// An optimistic branch in which more efficient copying is possible.
            if (offset + 16 * UNROLL_TIMES <= data.capacity() && istr.position() + size + 16 * UNROLL_TIMES <= istr.buffer().end())
            {
                const __m128i * sse_src_pos = reinterpret_cast<const __m128i *>(istr.position());
                const __m128i * sse_src_end = sse_src_pos + (size + (16 * UNROLL_TIMES - 1)) / 16 / UNROLL_TIMES * UNROLL_TIMES;
                __m128i * sse_dst_pos = reinterpret_cast<__m128i *>(&data[offset - size - 1]);

                while (sse_src_pos < sse_src_end)
                {
                    /// NOTE gcc 4.9.2 unrolls the loop, but for some reason uses only one xmm register.
                    /// for (size_t j = 0; j < UNROLL_TIMES; ++j)
                    ///    _mm_storeu_si128(sse_dst_pos + j, _mm_loadu_si128(sse_src_pos + j));

                    sse_src_pos += UNROLL_TIMES;
                    sse_dst_pos += UNROLL_TIMES;

                    if (UNROLL_TIMES >= 4)
                        __asm__("movdqu %0, %%xmm0" ::"m"(sse_src_pos[-4]));
                    if (UNROLL_TIMES >= 3)
                        __asm__("movdqu %0, %%xmm1" ::"m"(sse_src_pos[-3]));
                    if (UNROLL_TIMES >= 2)
                        __asm__("movdqu %0, %%xmm2" ::"m"(sse_src_pos[-2]));
                    if (UNROLL_TIMES >= 1)
                        __asm__("movdqu %0, %%xmm3" ::"m"(sse_src_pos[-1]));

                    if (UNROLL_TIMES >= 4)
                        __asm__("movdqu %%xmm0, %0"
                                : "=m"(sse_dst_pos[-4]));
                    if (UNROLL_TIMES >= 3)
                        __asm__("movdqu %%xmm1, %0"
                                : "=m"(sse_dst_pos[-3]));
                    if (UNROLL_TIMES >= 2)
                        __asm__("movdqu %%xmm2, %0"
                                : "=m"(sse_dst_pos[-2]));
                    if (UNROLL_TIMES >= 1)
                        __asm__("movdqu %%xmm3, %0"
                                : "=m"(sse_dst_pos[-1]));
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


void DataTypeString::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
    ColumnString & column_string = typeid_cast<ColumnString &>(column);
    ColumnString::Chars_t & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();

    double avg_chars_size;

    if (avg_value_size_hint && avg_value_size_hint > sizeof(offsets[0]))
    {
        /// Randomly selected.
        constexpr auto avg_value_size_hint_reserve_multiplier = 1.2;

        avg_chars_size = (avg_value_size_hint - sizeof(offsets[0])) * avg_value_size_hint_reserve_multiplier;
    }
    else
    {
        /// By default reserve only for empty strings.
        avg_chars_size = 1;
    }

    data.reserve(data.size() + std::ceil(limit * avg_chars_size));

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
    ColumnString & column_string = static_cast<ColumnString &>(column);
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


void DataTypeString::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON &) const
{
    writeJSONString(static_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


void DataTypeString::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
{
    read(column, [&](ColumnString::Chars_t & data) { readJSONStringInto(data, istr); });
}


void DataTypeString::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeXMLString(static_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


void DataTypeString::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeCSVString<>(static_cast<const ColumnString &>(column).getDataAt(row_num), ostr);
}


void DataTypeString::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char /*delimiter*/) const
{
    read(column, [&](ColumnString::Chars_t & data) { readCSVStringInto(data, istr); });
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

} // namespace DB
