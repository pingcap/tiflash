// TODO: Add copyright for PingCAP
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <Common/FailPoint.h>
#include <Common/RedactHelpers.h>
#include <Core/Defines.h>
#include <Encryption/EncryptionPath.h>
#include <Encryption/ReadBufferFromFileProvider.h>
#include <Encryption/createWriteBufferFromFileBaseByFileProvider.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/createReadBufferFromFileBase.h>
#include <Storages/Page/V3/LogFormat.h>
#include <Storages/Page/V3/LogReader.h>
#include <Storages/Page/V3/LogWriter.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/logger_useful.h>
#include <gtest/gtest.h>
#include <sys/types.h>

#include <cstring>
#include <memory>
#include <pcg_random.hpp>
#include <random>

#include "IO/WriteHelpers.h"


using DB::tests::TiFlashTestEnv;

namespace DB::FailPoints
{
extern const char exception_when_read_from_log[];
}

namespace DB::PS::V3::tests
{
// Construct a string of the specified length made out of the supplied
// partial string.
static std::string BigString(const std::string & partial_string, size_t n)
{
    std::string result;
    while (result.size() < n)
    {
        result.append(partial_string);
    }
    result.resize(n);
    return result;
}

static UInt32 getSkewedNum(int max_log, std::mt19937 & rd)
{
    pcg64 gen(rd());
    std::uniform_int_distribution<> d(0, max_log + 1);
    std::uniform_int_distribution<> d2(0, 1 << d(gen));
    return d2(gen);
}

// Return a skewed potentially long string
static String RandomSkewedString(int i, std::mt19937 & rd)
{
    return BigString(DB::toString(i), getSkewedNum(17, rd));
}

class StringSink : public DB::WriteBufferFromFileBase
{
public:
    String & contents;

    explicit StringSink(String & contents_)
        : DB::WriteBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0)
        , contents(contents_)
    {}

    off_t getPositionInFile() override { return count(); }
    void sync() override { next(); }
    String getFileName() const override { return ""; }
    int getFD() const override { return -1; }
    void close() override {}

protected:
    off_t doSeek(off_t off [[maybe_unused]], int whence [[maybe_unused]]) override { return getPositionInFile(); }
    void doTruncate(off_t length [[maybe_unused]]) override {}
    void nextImpl() override
    {
        if (offset() == 0)
            return;
        contents.append(working_buffer.begin(), offset());
    }
};
class OverwritingStringSink : public DB::WriteBufferFromFileBase
{
public:
    String & contents;

    explicit OverwritingStringSink(String & contents_)
        : DB::WriteBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0)
        , contents(contents_)
        , last_sync_pos(0)
    {}

    off_t getPositionInFile() override { return count(); }
    void sync() override { next(); }
    String getFileName() const override { return ""; }
    int getFD() const override { return -1; }
    void close() override {}

protected:
    off_t doSeek(off_t off [[maybe_unused]], int whence [[maybe_unused]]) override { return getPositionInFile(); }
    void doTruncate(off_t length [[maybe_unused]]) override {}
    void nextImpl() override
    {
        if (offset() == 0)
            return;
        if (last_sync_pos < contents.size())
        {
            size_t overwrite_size = std::min(contents.size() - last_sync_pos, offset());
            // overwrite
            memcpy(contents.data() + last_sync_pos, working_buffer.begin(), overwrite_size);
            // append the left over from working_buffer (if any)
            contents.append(working_buffer.begin() + overwrite_size, offset() - overwrite_size);
        }
        else
        {
            contents.append(working_buffer.begin(), offset());
        }
        last_sync_pos += offset();
    }

private:
    size_t last_sync_pos;
};

// Param type is tuple<int, bool>
// get<0>(tuple): non-zero if recycling log, zero if regular log
// get<1>(tuple): true if allow retry after read EOF, false otherwise
class WALLogTest : public ::testing::TestWithParam<std::tuple<bool, bool>>
{
private:
    class ReportCollector : public LogReader::Reporter
    {
    public:
        size_t dropped_bytes;
        String message;

        ReportCollector()
            : dropped_bytes(0)
        {}
        void corruption(size_t bytes, const String & msg) override
        {
            dropped_bytes += bytes;
            message.append(msg);
        }
    };

    class StringSouce : public DB::ReadBufferFromFileBase
    {
    public:
        String & contents;
        size_t read_pos;
        bool fail_after_read_partial;
        bool returned_partial;

        explicit StringSouce(String & contents_, bool fail_after_read_partial_)
            : DB::ReadBufferFromFileBase(PS::V3::Format::BLOCK_SIZE, nullptr, 0)
            , contents(contents_)
            , read_pos(0)
            , fail_after_read_partial(fail_after_read_partial_)
            , returned_partial(false)
        {}

        off_t getPositionInFile() override { return count(); }
        String getFileName() const override { return ""; }
        int getFD() const override { return -1; }

        off_t doSeek(off_t off [[maybe_unused]], int whence [[maybe_unused]]) override { return 0; }

    protected:
        bool nextImpl() override
        {
            if (fail_after_read_partial)
            {
                EXPECT_FALSE(returned_partial) << "must not Read() after eof/error";
            }

            // EOF
            if (read_pos >= contents.size())
                return false;

            std::string_view left_bytes{contents};
            left_bytes.remove_prefix(read_pos);
            // There are more bytes than buffer size, only copy a part of it, otherwise, copy to the end of `contents`
            const size_t num_bytes_read = std::min(internal_buffer.size(), left_bytes.size());
            memcpy(internal_buffer.begin(), left_bytes.data(), num_bytes_read);
            left_bytes.remove_prefix(num_bytes_read);
            read_pos += num_bytes_read;
            working_buffer.resize(num_bytes_read);
            return true;
        }
    };


    String reader_contents;
    ReportCollector report;
    std::unique_ptr<LogWriter> writer;
    std::unique_ptr<LogReader> reader;
    Poco::Logger * log;

protected:
    bool recyclable_log;
    bool allow_retry_read;
    const int log_file_num = 123;

public:
    WALLogTest()
        : log(&Poco::Logger::get("WALLogTest"))
        , recyclable_log(std::get<0>(GetParam()))
        , allow_retry_read(std::get<1>(GetParam()))
    {
        auto ctx = TiFlashTestEnv::getContext();
        auto provider = ctx.getFileProvider();
        auto filename = TiFlashTestEnv::getTemporaryPath("WALLogTest");

        std::unique_ptr<WriteBufferFromFileBase> file_writer = std::make_unique<StringSink>(reader_contents);
        writer = std::make_unique<LogWriter>(std::move(file_writer), /*log_num*/ log_file_num, /*recycle_log*/ recyclable_log);
        std::unique_ptr<ReadBufferFromFileBase> file_reader = std::make_unique<StringSouce>(reader_contents, /*fail_after_read_partial_*/ !allow_retry_read);
        reader = std::make_unique<LogReader>(std::move(file_reader), &report, /* verify_checksum */ true, /* log_number */ log_file_num, log);
    }

    void write(const std::string & msg)
    {
        ReadBufferFromString buff(msg);
        ASSERT_NO_THROW(writer->addRecord(buff, msg.size()));
    }

    size_t writtenBytes() const
    {
        return reader_contents.size();
    }

    std::string Read(const WALRecoveryMode wal_recovery_mode = WALRecoveryMode::TolerateCorruptedTailRecords)
    {
        if (auto [ok, scratch] = reader->readRecord(wal_recovery_mode); ok)
            return scratch;
        return "EOF";
    }

    void incrementByte(int offset, char delta)
    {
        reader_contents[offset] += delta;
    }

    void setByte(int offset, char new_byte)
    {
        reader_contents[offset] = new_byte;
    }

    void shrinkSize(int bytes)
    {
        reader_contents.resize(reader_contents.size() - bytes);
    }

    String & getReaderContents()
    {
        return reader_contents;
    }

    void fixChecksum(int header_offset, int len, bool recyclable)
    {
        // Compute crc of type/len/data
        int header_size = recyclable ? Format::RECYCLABLE_HEADER_SIZE : Format::HEADER_SIZE;
        Digest::CRC32 digest;
        digest.update(&reader_contents[header_offset + 6], header_size - 6 + len);
        auto checksum = digest.checksum();
        WriteBuffer buff(&reader_contents[header_offset], sizeof(checksum));
        writeIntBinary(checksum, buff);
    }

    size_t droppedBytes() const
    {
        return report.dropped_bytes;
    }

    std::string reportMessage() const
    {
        return report.message;
    }

    // Returns OK iff recorded error message contains "msg"
    std::string matchError(const std::string & msg) const
    {
        if (report.message.find(msg) == std::string::npos)
            return report.message;
        return "OK";
    }
};

TEST_P(WALLogTest, Empty)
{
    ASSERT_EQ("EOF", Read());
}

TEST_P(WALLogTest, ReadWrite)
{
    write("foo");
    write("bar");
    write("");
    write("xxxx");
    ASSERT_EQ("foo", Read());
    ASSERT_EQ("bar", Read());
    ASSERT_EQ("", Read());
    ASSERT_EQ("xxxx", Read());
    ASSERT_EQ("EOF", Read());
    ASSERT_EQ("EOF", Read()); // Make sure reads at eof work
}

TEST_P(WALLogTest, BlockBoundary)
{
    const auto big_str = BigString("A", PS::V3::Format::BLOCK_SIZE - Format::HEADER_SIZE - 4);
    write(big_str);
    write("small");
    ASSERT_EQ(big_str, Read());
    ASSERT_EQ("small", Read());
    ASSERT_EQ("EOF", Read());
}

TEST_P(WALLogTest, ManyBlocks)
{
    const size_t num_blocks_test = 100000;
    for (size_t i = 0; i < num_blocks_test; i++)
    {
        write(DB::toString(i));
    }
    for (size_t i = 0; i < num_blocks_test; i++)
    {
        auto res = Read();
        ASSERT_EQ(DB::toString(i), res);
    }
    ASSERT_EQ("EOF", Read());
}

TEST_P(WALLogTest, Fragmentation)
{
    write("small");
    write(BigString("medium", 50000));
    write(BigString("large", 100000));
    ASSERT_EQ("small", Read());
    ASSERT_EQ(BigString("medium", 50000), Read());
    ASSERT_EQ(BigString("large", 100000), Read());
    ASSERT_EQ("EOF", Read());
}

TEST_P(WALLogTest, MarginalTrailer)
{
    // Make a trailer that is exactly the same length as an empty record.
    int header_size = recyclable_log ? PS::V3::Format::RECYCLABLE_HEADER_SIZE : PS::V3::Format::HEADER_SIZE;
    const int n = PS::V3::Format::BLOCK_SIZE - 2 * header_size;
    write(BigString("foo", n));
    ASSERT_EQ(static_cast<size_t>(PS::V3::Format::BLOCK_SIZE - header_size), writtenBytes());
    write("");
    write("bar");
    ASSERT_EQ(BigString("foo", n), Read());
    ASSERT_EQ("", Read());
    ASSERT_EQ("bar", Read());
    ASSERT_EQ("EOF", Read());
}

TEST_P(WALLogTest, MarginalTrailer2)
{
    // Make a trailer that is exactly the same length as an empty record.
    int header_size = recyclable_log ? PS::V3::Format::RECYCLABLE_HEADER_SIZE : PS::V3::Format::HEADER_SIZE;
    const int n = PS::V3::Format::BLOCK_SIZE - 2 * header_size;
    write(BigString("foo", n));
    ASSERT_EQ((unsigned int)(PS::V3::Format::BLOCK_SIZE - header_size), writtenBytes());
    write("bar");
    ASSERT_EQ(BigString("foo", n), Read());
    ASSERT_EQ("bar", Read());
    ASSERT_EQ("EOF", Read());
    ASSERT_EQ(0, droppedBytes());
    ASSERT_EQ("", reportMessage());
}

TEST_P(WALLogTest, ShortTrailer)
{
    int header_size = recyclable_log ? PS::V3::Format::RECYCLABLE_HEADER_SIZE : PS::V3::Format::HEADER_SIZE;
    const int n = PS::V3::Format::BLOCK_SIZE - 2 * header_size + 4;
    write(BigString("foo", n));
    ASSERT_EQ((unsigned int)(PS::V3::Format::BLOCK_SIZE - header_size + 4), writtenBytes());
    write("");
    write("bar");
    ASSERT_EQ(BigString("foo", n), Read());
    ASSERT_EQ("", Read());
    ASSERT_EQ("bar", Read());
    ASSERT_EQ("EOF", Read());
}

TEST_P(WALLogTest, AlignedEof)
{
    int header_size = recyclable_log ? PS::V3::Format::RECYCLABLE_HEADER_SIZE : PS::V3::Format::HEADER_SIZE;
    const int n = PS::V3::Format::BLOCK_SIZE - 2 * header_size + 4;
    write(BigString("foo", n));
    ASSERT_EQ((unsigned int)(PS::V3::Format::BLOCK_SIZE - header_size + 4), writtenBytes());
    ASSERT_EQ(BigString("foo", n), Read());
    ASSERT_EQ("EOF", Read());
}

TEST_P(WALLogTest, RandomRead)
{
    constexpr int n = 500;
    constexpr int rand_seed = 301;
    std::mt19937 write_rd(rand_seed);
    for (int i = 0; i < n; i++)
    {
        write(RandomSkewedString(i, write_rd));
    }
    std::mt19937 read_rd(rand_seed);
    for (int i = 0; i < n; i++)
    {
        ASSERT_EQ(RandomSkewedString(i, read_rd), Read());
    }
    ASSERT_EQ("EOF", Read());
}

/// Tests of all the error paths in LogReader.cpp follow:

TEST_P(WALLogTest, ReadError)
{
    write("foo");
    FailPointHelper::enableFailPoint(::DB::FailPoints::exception_when_read_from_log);
    ASSERT_EQ("EOF", Read());
    ASSERT_EQ(PS::V3::Format::BLOCK_SIZE, droppedBytes());
    ASSERT_EQ("OK", matchError("exception_when_read_from_log"));
}

TEST_P(WALLogTest, BadRecordType)
{
    write("foo");
    // Type is stored in header[6], break the type
    incrementByte(6, 100);
    // Meeting a unknown type, consider its header size as `Format::HEADER_SIZE`
    fixChecksum(0, 3, /*recyclable*/ false);
    // Can not successfully read the BadRecord, and get dropped bytes, message reported
    ASSERT_EQ("EOF", Read());
    ASSERT_EQ(3, droppedBytes());
    ASSERT_EQ("OK", matchError("unknown record type"));
}

TEST_P(WALLogTest, TruncatedTrailingRecordIsIgnored)
{
    write("foo");
    shrinkSize(4); // Drop all payload as well as a header byte
    ASSERT_EQ("EOF", Read());
    // Truncated last record is ignored, not treated as an error
    ASSERT_EQ(0, droppedBytes());
    ASSERT_EQ("", reportMessage());
}

TEST_P(WALLogTest, TruncatedTrailingRecordIsNotIgnored)
{
    if (allow_retry_read)
    {
        // If read retry is allowed, then truncated trailing record should not
        // raise an error.
        return;
    }
    write("foo");
    shrinkSize(4); // Drop all payload as well as a header byte
    ASSERT_EQ("EOF", Read(WALRecoveryMode::AbsoluteConsistency));
    // Truncated last record is ignored, not treated as an error
    ASSERT_GT(droppedBytes(), 0);
    ASSERT_EQ("OK", matchError("Corruption: truncated header"));
}

TEST_P(WALLogTest, BadLength)
{
    if (allow_retry_read)
    {
        // If read retry is allowed, then we should not raise an error when the
        // record length specified in header is longer than data currently
        // available. It's possible that the body of the record is not written yet.
        return;
    }
    int header_size = recyclable_log ? PS::V3::Format::RECYCLABLE_HEADER_SIZE : PS::V3::Format::HEADER_SIZE;
    const int payload_size = PS::V3::Format::BLOCK_SIZE - header_size;
    write(BigString("bar", payload_size));
    write("foo");
    // Least significant size byte is stored in header[4].
    incrementByte(4, 1);
    if (!recyclable_log)
    {
        ASSERT_EQ("foo", Read());
        ASSERT_EQ(PS::V3::Format::BLOCK_SIZE, droppedBytes());
        ASSERT_EQ("OK", matchError("bad record length"));
    }
    else
    {
        ASSERT_EQ("EOF", Read());
    }
}

TEST_P(WALLogTest, BadLengthAtEndIsIgnored)
{
    if (allow_retry_read)
    {
        // If read retry is allowed, then we should not raise an error when the
        // record length specified in header is longer than data currently
        // available. It's possible that the body of the record is not written yet.
        return;
    }
    write("foo");
    shrinkSize(1);
    ASSERT_EQ("EOF", Read());
    ASSERT_EQ(0, droppedBytes());
    ASSERT_EQ("", reportMessage());
}

TEST_P(WALLogTest, BadLengthAtEndIsNotIgnored)
{
    if (allow_retry_read)
    {
        // If read retry is allowed, then we should not raise an error when the
        // record length specified in header is longer than data currently
        // available. It's possible that the body of the record is not written yet.
        return;
    }
    write("foo");
    shrinkSize(1);
    ASSERT_EQ("EOF", Read(WALRecoveryMode::AbsoluteConsistency));
    ASSERT_GT(droppedBytes(), 0);
    ASSERT_EQ("OK", matchError("Corruption: truncated record body"));
}

TEST_P(WALLogTest, ChecksumMismatch)
{
    write("foooooo");
    incrementByte(0, 14);
    ASSERT_EQ("EOF", Read());
    if (!recyclable_log)
    {
        ASSERT_EQ(14, droppedBytes());
        ASSERT_EQ("OK", matchError("checksum mismatch"));
    }
    else
    {
        ASSERT_EQ(0, droppedBytes());
        ASSERT_EQ("", reportMessage());
    }
}

TEST_P(WALLogTest, UnexpectedMiddleType)
{
    write("foo");
    setByte(6, static_cast<char>(recyclable_log ? Format::RecyclableMiddleType : Format::MiddleType));
    fixChecksum(0, 3, !!recyclable_log);
    ASSERT_EQ("EOF", Read());
    ASSERT_EQ(3, droppedBytes());
    ASSERT_EQ("OK", matchError("missing start"));
}

TEST_P(WALLogTest, UnexpectedLastType)
{
    write("foo");
    setByte(6, static_cast<char>(recyclable_log ? Format::RecyclableLastType : Format::LastType));
    fixChecksum(0, 3, recyclable_log);
    ASSERT_EQ("EOF", Read());
    ASSERT_EQ(3, droppedBytes());
    ASSERT_EQ("OK", matchError("missing start"));
}

TEST_P(WALLogTest, UnexpectedFullType)
{
    write("foo");
    write("bar");
    setByte(6, static_cast<char>(recyclable_log ? Format::RecyclableFirstType : Format::FirstType));
    fixChecksum(0, 3, recyclable_log);
    ASSERT_EQ("bar", Read());
    ASSERT_EQ("EOF", Read());
    ASSERT_EQ(3, droppedBytes());
    ASSERT_EQ("OK", matchError("partial record without end"));
}

TEST_P(WALLogTest, UnexpectedFirstType)
{
    write("foo");
    write(BigString("bar", 100000));
    setByte(6, static_cast<char>(recyclable_log ? Format::RecyclableFirstType : Format::FirstType));
    fixChecksum(0, 3, recyclable_log);
    ASSERT_EQ(BigString("bar", 100000), Read());
    ASSERT_EQ("EOF", Read());
    ASSERT_EQ(3, droppedBytes());
    ASSERT_EQ("OK", matchError("partial record without end"));
}

TEST_P(WALLogTest, MissingLastIsIgnored)
{
    write(BigString("bar", PS::V3::Format::BLOCK_SIZE));
    // Remove the LAST block, including header.
    shrinkSize(14);
    ASSERT_EQ("EOF", Read());
    ASSERT_EQ("", reportMessage());
    ASSERT_EQ(0, droppedBytes());
}

TEST_P(WALLogTest, MissingLastIsNotIgnored)
{
    if (allow_retry_read)
    {
        // If read retry is allowed, then truncated trailing record should not
        // raise an error.
        return;
    }
    write(BigString("bar", PS::V3::Format::BLOCK_SIZE));
    // Remove the LAST block, including header.
    shrinkSize(14);
    ASSERT_EQ("EOF", Read(WALRecoveryMode::AbsoluteConsistency));
    ASSERT_GT(droppedBytes(), 0);
    ASSERT_EQ("OK", matchError("Corruption: error reading trailing data"));
}

TEST_P(WALLogTest, PartialLastIsIgnored)
{
    write(BigString("bar", PS::V3::Format::BLOCK_SIZE));
    // Cause a bad record length in the LAST block.
    shrinkSize(1);
    ASSERT_EQ("EOF", Read());
    ASSERT_EQ("", reportMessage());
    ASSERT_EQ(0, droppedBytes());
}

TEST_P(WALLogTest, PartialLastIsNotIgnored)
{
    if (allow_retry_read)
    {
        // If read retry is allowed, then truncated trailing record should not
        // raise an error.
        return;
    }
    write(BigString("bar", PS::V3::Format::BLOCK_SIZE));
    // Cause a bad record length in the LAST block.
    shrinkSize(1);
    ASSERT_EQ("EOF", Read(WALRecoveryMode::AbsoluteConsistency));
    ASSERT_GT(droppedBytes(), 0);
    ASSERT_EQ("OK", matchError("Corruption: truncated record body"));
}

TEST_P(WALLogTest, ErrorJoinsRecords)
{
    // Consider two fragmented records:
    //    first(R1) last(R1) first(R2) last(R2)
    // where the middle two fragments disappear.  We do not want
    // first(R1),last(R2) to get joined and returned as a valid record.

    // write records that span two blocks
    write(BigString("foo", PS::V3::Format::BLOCK_SIZE));
    write(BigString("bar", PS::V3::Format::BLOCK_SIZE));
    write("correct");

    // Wipe the middle block
    for (unsigned int offset = PS::V3::Format::BLOCK_SIZE; offset < 2 * PS::V3::Format::BLOCK_SIZE; offset++)
    {
        setByte(offset, 'x');
    }

    if (!recyclable_log)
    {
        ASSERT_EQ("correct", Read());
        ASSERT_EQ("EOF", Read());
        size_t dropped = droppedBytes();
        ASSERT_LE(dropped, 2 * PS::V3::Format::BLOCK_SIZE + 100);
        ASSERT_GE(dropped, 2 * PS::V3::Format::BLOCK_SIZE);
    }
    else
    {
        ASSERT_EQ("EOF", Read());
    }
}

TEST_P(WALLogTest, Recycle)
{
    if (!recyclable_log)
    {
        return; // test is only valid for recycled logs
    }
    write("foo");
    write("bar");
    write("baz");
    write("bif");
    write("blitz");
    while (getReaderContents().size() < PS::V3::Format::BLOCK_SIZE * 2)
    {
        write("xxxxxxxxxxxxxxxx");
    }
    const size_t content_size_before_overwrite = getReaderContents().size();

    // Overwrite some record with same log file number
    std::unique_ptr<WriteBufferFromFileBase> file_writer = std::make_unique<OverwritingStringSink>(getReaderContents());
    std::unique_ptr<LogWriter> recycle_writer = std::make_unique<LogWriter>(std::move(file_writer), /*log_num*/ log_file_num, /*recycle_log*/ recyclable_log);
    String text_to_write = "foooo";
    ReadBufferFromString foo(text_to_write);
    recycle_writer->addRecord(foo, text_to_write.size());
    text_to_write = "bar";
    ReadBufferFromString bar(text_to_write);
    recycle_writer->addRecord(bar, text_to_write.size());

    // Check that we should only read new records overwrited (with the same log number)
    ASSERT_GE(getReaderContents().size(), PS::V3::Format::BLOCK_SIZE * 2);
    ASSERT_EQ(getReaderContents().size(), content_size_before_overwrite);
    ASSERT_EQ("foooo", Read());
    ASSERT_EQ("bar", Read());
    ASSERT_EQ("EOF", Read());
}

TEST_P(WALLogTest, RecycleWithAnotherLogNum)
{
    if (!recyclable_log)
    {
        return; // test is only valid for recycled logs
    }
    write("foo");
    write("bar");
    write("baz");
    write("bif");
    write("blitz");
    while (getReaderContents().size() < PS::V3::Format::BLOCK_SIZE * 2)
    {
        write("xxxxxxxxxxxxxxxx");
    }
    const size_t content_size_before_overwrite = getReaderContents().size();

    // Overwrite some record with another log file number
    size_t overwrite_log_num = log_file_num + 1;
    std::unique_ptr<WriteBufferFromFileBase> file_writer = std::make_unique<OverwritingStringSink>(getReaderContents());
    std::unique_ptr<LogWriter> recycle_writer = std::make_unique<LogWriter>(std::move(file_writer), /*log_num*/ overwrite_log_num, /*recycle_log*/ recyclable_log);
    String text_to_write = "foooo";
    ReadBufferFromString foo(text_to_write);
    recycle_writer->addRecord(foo, text_to_write.size());
    text_to_write = "bar";
    ReadBufferFromString bar(text_to_write);
    recycle_writer->addRecord(bar, text_to_write.size());

    ASSERT_GE(getReaderContents().size(), PS::V3::Format::BLOCK_SIZE * 2);
    ASSERT_EQ(getReaderContents().size(), content_size_before_overwrite);
    // Read with old log number
    ASSERT_EQ("EOF", Read());
    // TODO: Read with new log number
}

TEST_P(WALLogTest, RecycleWithSameBoundaryLogNum)
{
    if (!recyclable_log)
    {
        return; // test is only valid for recycled logs
    }
    write("foo");
    write("bar");
    size_t boundary = getReaderContents().size();
    write("baz");
    write("bif");
    write("blitz");
    size_t num_writes_stuff = 0;
    while (getReaderContents().size() < PS::V3::Format::BLOCK_SIZE * 2)
    {
        write("xxxxxxxxxxxxxxxx");
        num_writes_stuff++;
    }
    const size_t content_size_before_overwrite = getReaderContents().size();

    // Overwrite some record with same log file number
    std::unique_ptr<WriteBufferFromFileBase> file_writer = std::make_unique<OverwritingStringSink>(getReaderContents());
    std::unique_ptr<LogWriter> recycle_writer = std::make_unique<LogWriter>(std::move(file_writer), /*log_num*/ log_file_num, /*recycle_log*/ recyclable_log);
    String text_to_write = BigString("A", boundary - PS::V3::Format::RECYCLABLE_HEADER_SIZE);
    ReadBufferFromString foo(text_to_write);
    recycle_writer->addRecord(foo, text_to_write.size());

    ASSERT_GE(getReaderContents().size(), PS::V3::Format::BLOCK_SIZE * 2);
    ASSERT_EQ(getReaderContents().size(), content_size_before_overwrite);
    // Read with old log number
    ASSERT_EQ(text_to_write, Read());
    ASSERT_EQ("baz", Read());
    ASSERT_EQ("bif", Read());
    ASSERT_EQ("blitz", Read());
    while (num_writes_stuff--)
    {
        ASSERT_EQ("xxxxxxxxxxxxxxxx", Read());
    }
    ASSERT_EQ("EOF", Read());
}

INSTANTIATE_TEST_CASE_P(
    Recycle_AllowRetryRead,
    WALLogTest,
    ::testing::Combine(
        ::testing::Bool(),
        ::testing::Bool()),
    [](const ::testing::TestParamInfo<WALLogTest::ParamType> & info) -> String {
        const auto [recycle_log, allow_retry_read] = info.param;
        return fmt::format("{}_{}", recycle_log, allow_retry_read);
    });

} // namespace DB::PS::V3::tests
