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
#include <Common/config.h> // for ENABLE_NEXT_GEN
#include <IO/Buffer/WriteBufferFromString.h>
#include <Storages/KVStore/TiKVHelpers/DecodedLockCFValue.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <functional>

namespace DB::tests
{
namespace
{
void expectRejected(const std::function<void()> & read, int code)
{
    try
    {
        read();
        FAIL() << "expected the read to be rejected";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), code) << e.message();
    }
}

TiKVValue buildBytes(const std::function<void(WriteBufferFromOwnString &)> & write)
{
    WriteBufferFromOwnString buf;
    write(buf);
    return TiKVValue(buf.releaseStr());
}

std::shared_ptr<const TiKVKey> lockKey()
{
    return std::make_shared<const TiKVKey>(RecordKVFormat::genKey(1, 1));
}
} // namespace

TEST(TiKVRecordFormatBoundsTest, RejectsFixedWidthReadsPastEnd)
try
{
    const char byte = 'P';
    const char * data = &byte;
    size_t len = 0;
    expectRejected([&] { RecordKVFormat::readUInt8(data, len); }, ErrorCodes::LOGICAL_ERROR);
    EXPECT_EQ(data, &byte);
    EXPECT_EQ(len, 0);

    data = &byte;
    len = 1;
    EXPECT_EQ(RecordKVFormat::readUInt8(data, len), static_cast<UInt8>('P'));
    EXPECT_EQ(len, 0);

    char raw[7] = {};
    data = raw;
    len = sizeof(raw);
    expectRejected([&] { RecordKVFormat::readUInt64(data, len); }, ErrorCodes::LOGICAL_ERROR);
    EXPECT_EQ(data, raw);
    EXPECT_EQ(len, sizeof(raw));
}
CATCH

TEST(TiKVRecordFormatBoundsTest, RejectsStringLongerThanRemaining)
try
{
    const char payload[] = {'a', 'b'};
    const char * data = payload;
    size_t len = sizeof(payload);
    expectRejected([&] { RecordKVFormat::readRawString<std::string_view>(data, len, 3); }, ErrorCodes::LOGICAL_ERROR);
    EXPECT_EQ(data, payload);
    EXPECT_EQ(len, sizeof(payload));

    data = payload;
    len = sizeof(payload);
    EXPECT_EQ(RecordKVFormat::readRawString<std::string_view>(data, len, 2), "ab");
    EXPECT_EQ(len, 0);

    // A negative zigzag length becomes a huge size_t and must be rejected before any copy.
    auto negative = buildBytes([](WriteBufferFromOwnString & buf) { TiKV::writeVarInt(static_cast<Int64>(-1), buf); });
    data = negative.data();
    len = negative.dataSize();
    expectRejected([&] { RecordKVFormat::readVarString<std::string_view>(data, len); }, ErrorCodes::LOGICAL_ERROR);

    auto too_long = buildBytes([](WriteBufferFromOwnString & buf) { TiKV::writeVarInt(static_cast<Int64>(10), buf); });
    data = too_long.data();
    len = too_long.dataSize();
    expectRejected([&] { RecordKVFormat::readVarString<std::string_view>(data, len); }, ErrorCodes::LOGICAL_ERROR);
}
CATCH

TEST(TiKVRecordFormatBoundsTest, RejectsMalformedLockCfValue)
try
{
    auto key = lockKey();
    auto empty = std::make_shared<const TiKVValue>("");
    expectRejected([&] { RecordKVFormat::DecodedLockCFValue(key, empty); }, ErrorCodes::LOGICAL_ERROR);

    auto type_only = std::make_shared<const TiKVValue>(buildBytes(
        [](WriteBufferFromOwnString & buf) { buf.write(static_cast<char>(RecordKVFormat::LockType::Put)); }));
    expectRejected([&] { RecordKVFormat::DecodedLockCFValue(key, type_only); }, ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);

    auto primary_too_long = std::make_shared<const TiKVValue>(buildBytes([](WriteBufferFromOwnString & buf) {
        buf.write(static_cast<char>(RecordKVFormat::LockType::Put));
        TiKV::writeVarInt(static_cast<Int64>(100), buf);
    }));
    expectRejected([&] { RecordKVFormat::DecodedLockCFValue(key, primary_too_long); }, ErrorCodes::LOGICAL_ERROR);

    auto negative_primary = std::make_shared<const TiKVValue>(buildBytes([](WriteBufferFromOwnString & buf) {
        buf.write(static_cast<char>(RecordKVFormat::LockType::Put));
        TiKV::writeVarInt(static_cast<Int64>(-1), buf);
    }));
    expectRejected([&] { RecordKVFormat::DecodedLockCFValue(key, negative_primary); }, ErrorCodes::LOGICAL_ERROR);

    auto short_commit_ts = std::make_shared<const TiKVValue>(buildBytes([](WriteBufferFromOwnString & buf) {
        buf.write(static_cast<char>(RecordKVFormat::LockType::Put));
        TiKV::writeVarInt(static_cast<Int64>(2), buf);
        buf.write("pk", 2);
        TiKV::writeVarUInt(static_cast<UInt64>(1), buf);
        TiKV::writeVarUInt(static_cast<UInt64>(1), buf);
        buf.write(RecordKVFormat::MIN_COMMIT_TS_PREFIX);
        buf.write("abc", 3);
    }));
    expectRejected([&] { RecordKVFormat::DecodedLockCFValue(key, short_commit_ts); }, ErrorCodes::LOGICAL_ERROR);
}
CATCH

TEST(TiKVRecordFormatBoundsTest, DecodesValidLockCfValue)
try
{
    auto raw = RecordKVFormat::encodeLockCfValue(RecordKVFormat::LockType::Put, "pk", 7, 9);
    auto key = lockKey();
    auto value = std::make_shared<const TiKVValue>(std::move(raw));
    RecordKVFormat::DecodedLockCFValue lock(key, value);
    auto info = lock.intoLockInfo();
    ASSERT_EQ(info->primary_lock(), "pk");
    ASSERT_EQ(info->lock_version(), 7);
    ASSERT_EQ(info->lock_ttl(), 9);
    ASSERT_EQ(info->lock_type(), kvrpcpb::Op::Put);
}
CATCH

TEST(TiKVRecordFormatBoundsTest, RejectsTruncatedWriteCfValue)
try
{
    auto missing_ts = buildBytes(
        [](WriteBufferFromOwnString & buf) { buf.write(static_cast<char>(RecordKVFormat::CFModifyFlag::PutFlag)); });
    expectRejected([&] { RecordKVFormat::decodeWriteCfValue(missing_ts); }, ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);

    auto short_value = buildBytes([](WriteBufferFromOwnString & buf) {
        buf.write(static_cast<char>(RecordKVFormat::CFModifyFlag::PutFlag));
        TiKV::writeVarUInt(static_cast<UInt64>(1), buf);
        buf.write(RecordKVFormat::SHORT_VALUE_PREFIX);
#if ENABLE_NEXT_GEN
        TiKV::writeVarUInt(static_cast<UInt64>(10), buf);
#else
        buf.write(static_cast<char>(10));
#endif
        buf.write("x", 1);
    });
    expectRejected([&] { RecordKVFormat::decodeWriteCfValue(short_value); }, ErrorCodes::LOGICAL_ERROR);

    auto truncated_last_change = buildBytes([](WriteBufferFromOwnString & buf) {
        buf.write(static_cast<char>(RecordKVFormat::CFModifyFlag::PutFlag));
        TiKV::writeVarUInt(static_cast<UInt64>(1), buf);
        buf.write(RecordKVFormat::LAST_CHANGE_PREFIX);
        buf.write("abc", 3);
    });
    expectRejected([&] { RecordKVFormat::decodeWriteCfValue(truncated_last_change); }, ErrorCodes::LOGICAL_ERROR);

    auto valid = RecordKVFormat::encodeWriteCfValue(RecordKVFormat::CFModifyFlag::PutFlag, 4, "value");
    auto decoded = RecordKVFormat::decodeWriteCfValue(valid);
    ASSERT_TRUE(decoded);
    ASSERT_EQ(decoded->prewrite_ts, 4);
    ASSERT_EQ(std::string_view(decoded->short_value->data(), decoded->short_value->dataSize()), "value");
}
CATCH
} // namespace DB::tests
