// Copyright 2026 PingCAP, Inc.
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

#include <Common/Exception.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <TiDB/Decode/DatumCodec.h>
#include <TiDB/Schema/TiDB.h>
#include <gtest/gtest.h>

#include <limits>

namespace DB::tests
{
namespace
{
String encodeTestDatum(const Field & value, TiDB::CodecFlag flag)
{
    WriteBufferFromOwnString buffer;
    EncodeDatum(value, flag, buffer);
    return buffer.releaseStr();
}

void expectDecodedAndSkipped(const Field & value, TiDB::CodecFlag flag)
{
    const auto encoded = encodeTestDatum(value, flag);
    for (const auto & prefix : {String{}, String{"prefix"}})
    {
        for (const auto & suffix : {String{}, String{"suffix"}})
        {
            const auto data = prefix + encoded + suffix;
            size_t cursor = prefix.size();
            EXPECT_EQ(DecodeDatum(cursor, data), value);
            EXPECT_EQ(cursor, prefix.size() + encoded.size());

            cursor = prefix.size();
            EXPECT_NO_THROW(SkipDatum(cursor, data));
            EXPECT_EQ(cursor, prefix.size() + encoded.size());
        }
    }
}

void expectInvalidDatum(const String & encoded)
{
    for (const auto & prefix : {String{}, String{"prefix"}})
    {
        const auto data = prefix + encoded;
        size_t cursor = prefix.size();
        EXPECT_THROW(DecodeDatum(cursor, data), Exception);

        cursor = prefix.size();
        EXPECT_THROW(SkipDatum(cursor, data), Exception);
    }
}
} // namespace

TEST(DatumCodecTest, CompactBytesDecodesAndSkipsValidValues)
{
    for (const auto & value : {String{}, String{"hello"}, String{"a\0b", 3}, String(128, 'x')})
        expectDecodedAndSkipped(value, TiDB::CodecFlagCompactBytes);
}

TEST(DatumCodecTest, CompactBytesRejectsEveryTruncatedPrefix)
{
    const auto encoded = encodeTestDatum(String(128, 'x'), TiDB::CodecFlagCompactBytes);
    for (size_t size = 1; size < encoded.size(); ++size)
    {
        SCOPED_TRACE(size);
        expectInvalidDatum(encoded.substr(0, size));
    }
}

TEST(DatumCodecTest, CompactBytesRejectsInvalidLengths)
{
    for (const Int64 length : {Int64{-1}, std::numeric_limits<Int64>::min(), std::numeric_limits<Int64>::max()})
    {
        SCOPED_TRACE(length);
        WriteBufferFromOwnString buffer;
        buffer.write(static_cast<char>(TiDB::CodecFlagCompactBytes));
        EncodeVarInt(length, buffer);
        buffer.write("payload", 7);
        expectInvalidDatum(buffer.releaseStr());
    }
}

TEST(DatumCodecTest, VectorFloat32DecodesAndSkipsValidValues)
{
    for (const auto & value : {Array{}, Array{1.5}, Array{1.5, -2.25, 0.0}})
        expectDecodedAndSkipped(value, TiDB::CodecFlagVectorFloat32);
}

TEST(DatumCodecTest, VectorFloat32RejectsEveryTruncatedPrefix)
{
    const auto encoded = encodeTestDatum(Array{1.5, -2.25, 0.0}, TiDB::CodecFlagVectorFloat32);
    for (size_t size = 1; size < encoded.size(); ++size)
    {
        SCOPED_TRACE(size);
        expectInvalidDatum(encoded.substr(0, size));
    }
}

TEST(DatumCodecTest, VectorFloat32RejectsOversizedLength)
{
    WriteBufferFromOwnString buffer;
    buffer.write(static_cast<char>(TiDB::CodecFlagVectorFloat32));
    writeIntBinary(toLittleEndian(std::numeric_limits<UInt32>::max()), buffer);
    expectInvalidDatum(buffer.releaseStr());
}

TEST(DatumCodecTest, RejectsInvalidCursor)
{
    const String data(sizeof(UInt32), '\0');
    for (const size_t offset : {data.size(), data.size() + 1, std::numeric_limits<size_t>::max()})
    {
        SCOPED_TRACE(offset);
        size_t cursor = offset;
        EXPECT_THROW(DecodeCompactBytes(cursor, data), Exception);

        cursor = offset;
        EXPECT_THROW(DecodeVectorFloat32(cursor, data), Exception);

        cursor = offset;
        EXPECT_THROW(DecodeInt64(cursor, data), Exception);

        cursor = offset;
        EXPECT_THROW(DecodeDecimal(cursor, data), Exception);
    }
}

TEST(DatumCodecTest, FixedWidthDatumDecodesAndSkipsValidValues)
{
    for (const auto value : {Int64{-1}, Int64{0}, Int64{42}})
    {
        expectDecodedAndSkipped(value, TiDB::CodecFlagInt);
        expectDecodedAndSkipped(value, TiDB::CodecFlagDuration);
    }
    for (const auto value : {UInt64{0}, UInt64{42}, UInt64{std::numeric_limits<UInt64>::max()}})
        expectDecodedAndSkipped(value, TiDB::CodecFlagUInt);
    for (const auto value : {Float64{-1.5}, Float64{0.0}, Float64{1.5}})
        expectDecodedAndSkipped(value, TiDB::CodecFlagFloat);
}

TEST(DatumCodecTest, FixedWidthDatumRejectsEveryTruncatedPrefix)
{
    for (const auto & encoded : {
             encodeTestDatum(Int64{42}, TiDB::CodecFlagInt),
             encodeTestDatum(UInt64{42}, TiDB::CodecFlagUInt),
             encodeTestDatum(Float64{1.5}, TiDB::CodecFlagFloat),
             encodeTestDatum(Int64{42}, TiDB::CodecFlagDuration),
         })
    {
        for (size_t size = 1; size < encoded.size(); ++size)
        {
            SCOPED_TRACE(size);
            size_t cursor = 0;
            EXPECT_THROW(DecodeDatum(cursor, encoded.substr(0, size)), Exception);

            cursor = 0;
            EXPECT_THROW(SkipDatum(cursor, encoded.substr(0, size)), Exception);
        }
    }
}

TEST(DatumCodecTest, DecimalDatumDecodesAndSkipsValidValues)
{
    const DecimalField<Decimal64> value{Decimal64{12345}, 0};
    const auto encoded = encodeTestDatum(value, TiDB::CodecFlagDecimal);
    size_t cursor = 0;
    const auto decoded = DecodeDatum(cursor, encoded);
    EXPECT_EQ(cursor, encoded.size());
    EXPECT_EQ(static_cast<Int64>(decoded.get<DecimalField<Decimal32>>()), 12345);

    cursor = 0;
    EXPECT_NO_THROW(SkipDatum(cursor, encoded));
    EXPECT_EQ(cursor, encoded.size());
}

TEST(DatumCodecTest, DecimalDatumRejectsEveryTruncatedPrefix)
{
    const DecimalField<Decimal64> value{Decimal64{12345}, 0};
    const auto encoded = encodeTestDatum(value, TiDB::CodecFlagDecimal);
    for (size_t size = 1; size < encoded.size(); ++size)
    {
        SCOPED_TRACE(size);
        size_t cursor = 0;
        EXPECT_THROW(DecodeDatum(cursor, encoded.substr(0, size)), Exception);

        cursor = 0;
        EXPECT_THROW(SkipDatum(cursor, encoded.substr(0, size)), Exception);
    }
}

TEST(DatumCodecTest, SkipDatumRejectsMalformedDecimalDatum)
{
    for (const auto & value : {
             String{static_cast<char>(TiDB::CodecFlagDecimal), '\x01', '\x05'}, // precision < scale
             String{static_cast<char>(TiDB::CodecFlagDecimal), '\x00', '\x00'}, // zero precision
             String{static_cast<char>(TiDB::CodecFlagDecimal), '\x05', '\x02'}, // truncated body
         })
    {
        size_t cursor = 0;
        EXPECT_THROW(SkipDatum(cursor, value), Exception);
    }
}

TEST(DatumCodecTest, DecimalForCHRowRejectsMalformedDatum)
{
    TiDB::ColumnInfo column_info;
    column_info.flen = 10;
    column_info.decimal = 2;
    for (const auto & value : {
             String{static_cast<char>(TiDB::CodecFlagDecimal), '\x05'}, // truncated header
             String{static_cast<char>(TiDB::CodecFlagDecimal), '\x05', '\x09'}, // precision < scale
             String{static_cast<char>(TiDB::CodecFlagDecimal), '\x05', '\x02'}, // truncated body
             // precision exceeds the capacity of the column's decimal type (Decimal64, max 18)
             String{static_cast<char>(TiDB::CodecFlagDecimal), '\x13', '\x00'},
         })
    {
        size_t cursor = 0;
        EXPECT_THROW(DecodeDatumForCHRow(cursor, value, column_info), Exception);
    }
}
} // namespace DB::tests
