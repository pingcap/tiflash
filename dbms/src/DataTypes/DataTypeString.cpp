// Copyright 2022 PingCAP, Ltd.
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
#include <Common/typeid_cast.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <common/simd.h>

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


template <size_t BLOCK_SIZE>
static ALWAYS_INLINE inline void deserializeBinaryBlockImpl(ColumnString::Chars_t & data, ColumnString::Offsets & offsets, ReadBuffer & istr, size_t limit)
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

#ifdef __x86_64__
        // The intel reference manual states that for sizes larger than 128, using REP MOVSB will give identical performance with other variants.
        // Beginning from 2048 bytes, REP MOVSB gives an even better performance.
        if constexpr (BLOCK_SIZE >= 256)
        {
            /*
             * According to intel's reference manual:
             *
             * On older microarchitecture (ivy bridge), a REP MOVSB implementation of memcpy can achieve throughput at
             * slightly better than the 128-bit SIMD implementation when copying thousands of bytes.
             *
             * On newer microarchitecture (haswell), using REP MOVSB to implement memcpy operation for large copy length
             * can take advantage the 256-bit store data path and deliver throughput of more than 20 bytes per cycle.
             * For copy length that are smaller than a few hundred bytes, REP MOVSB approach is still slower than those
             * SIMD approaches.
             */
            if (size >= 1024 && common::cpu_feature_flags.erms && istr.position() + size <= istr.buffer().end())
            {
                const auto * src = reinterpret_cast<const char *>(istr.position());
                auto * dst = reinterpret_cast<char *>(&data[offset - size - 1]);
                istr.position() += size;
                /*
                 *  For destination buffer misalignment:
                 *  The impact on Enhanced REP MOVSB and STOSB implementation can be 25%
                 *  degradation, while 128-bit AVX implementation of memcpy may degrade only
                 *  5%, relative to 16-byte aligned scenario.
                 *
                 *  Therefore, we manually align up the destination buffer before startup.
                 */
                tiflash_compiler_builtin_memcpy(dst, src, 64);
                auto address = reinterpret_cast<uintptr_t>(dst);
                auto shift = 64 - (address % 64);
                dst += shift;
                src += shift;
                size -= shift;
                asm volatile("rep movsb"
                             : "+D"(dst), "+S"(src), "+c"(size)
                             :
                             : "memory");
                data[offset - 1] = 0;
                continue;
            }
        }
#endif
        if (size)
        {
            /// An optimistic branch in which more efficient copying is possible.
            if (offset + BLOCK_SIZE <= data.capacity() && istr.position() + size + BLOCK_SIZE <= istr.buffer().end())
            {
                const auto * src = reinterpret_cast<const char *>(istr.position());
                const auto * target = src + (size + BLOCK_SIZE - 1) / BLOCK_SIZE * BLOCK_SIZE;
                auto * dst = reinterpret_cast<char *>(&data[offset - size - 1]);

                while (src < target)
                {
                    /**
                     * Compiler expands the builtin memcpy perfectly. There is no need to
                     * manually write SSE2 code for x86 here; moreover, this method can also bring
                     * optimization to aarch64 targets.
                     *
                     * (x86 loop body for 64 bytes version with XMM registers)
                     *     movups  xmm0, xmmword ptr [rsi]
                     *     movups  xmm1, xmmword ptr [rsi + 16]
                     *     movups  xmm2, xmmword ptr [rsi + 32]
                     *     movups  xmm3, xmmword ptr [rsi + 48]
                     *     movups  xmmword ptr [rdi + 48], xmm3
                     *     movups  xmmword ptr [rdi + 32], xmm2
                     *     movups  xmmword ptr [rdi + 16], xmm1
                     *     movups  xmmword ptr [rdi], xmm0
                     *
                     * (aarch64 loop body for 64 bytes version with Q registers)
                     *     ldp     q0, q1, [x1]
                     *     ldp     q2, q3, [x1, #32]
                     *     stp     q0, q1, [x0]
                     *     add     x1, x1, #64
                     *     cmp     x1, x8
                     *     stp     q2, q3, [x0, #32]
                     *     add     x0, x0, #64
                     */
                    tiflash_compiler_builtin_memcpy(dst, src, BLOCK_SIZE);
                    src += BLOCK_SIZE;
                    dst += BLOCK_SIZE;
                }
                istr.position() += size;
            }
            else
            {
                istr.readStrict(reinterpret_cast<char *>(&data[offset - size - 1]), size);
            }
        }

        data[offset - 1] = 0;
    }
}
#define TIFLASH_DESERIALIZE_VARIANT(SIZE)                                                                 \
    TIFLASH_MULTIVERSIONED_VECTORIZATION(                                                                 \
        void,                                                                                             \
        deserializeBinaryBlock##SIZE,                                                                     \
        (ColumnString::Chars_t & data, ColumnString::Offsets & offsets, ReadBuffer & istr, size_t limit), \
        (data, offsets, istr, limit),                                                                     \
        {                                                                                                 \
            return deserializeBinaryBlockImpl<SIZE>(data, offsets, istr, limit);                          \
        })

TIFLASH_DESERIALIZE_VARIANT(16)
TIFLASH_DESERIALIZE_VARIANT(32)
TIFLASH_DESERIALIZE_VARIANT(48)
TIFLASH_DESERIALIZE_VARIANT(64)
TIFLASH_DESERIALIZE_VARIANT(128)
TIFLASH_DESERIALIZE_VARIANT(256)
#undef TIFLASH_DESERIALIZE_VARIANT

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

    if (avg_chars_size >= 256)
        deserializeBinaryBlock256TiFlashMultiVersion::invoke(data, offsets, istr, limit);
    else if (avg_chars_size >= 128)
        deserializeBinaryBlock128TiFlashMultiVersion::invoke(data, offsets, istr, limit);
    else if (avg_chars_size >= 64)
        deserializeBinaryBlock64TiFlashMultiVersion::invoke(data, offsets, istr, limit);
    else if (avg_chars_size >= 48)
        deserializeBinaryBlock48TiFlashMultiVersion::invoke(data, offsets, istr, limit);
    else if (avg_chars_size >= 32)
        deserializeBinaryBlock32TiFlashMultiVersion::invoke(data, offsets, istr, limit);
    else
        deserializeBinaryBlock16TiFlashMultiVersion::invoke(data, offsets, istr, limit);
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
