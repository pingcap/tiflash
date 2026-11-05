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
    }
}
} // namespace DB::tests
