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

// Construct a string from a number
static std::string NumberString(int n)
{
    char buf[50];
    snprintf(buf, sizeof(buf), "%d.", n);
    return std::string(buf);
}

static uint32_t getSkewedNum(int max_log, std::mt19937 & rd)
{
    pcg64 gen(rd());
    std::uniform_int_distribution<> d(0, max_log + 1);
    std::uniform_int_distribution<> d2(0, 1 << d(gen));
    return d2(gen);
}

// Return a skewed potentially long string
static std::string RandomSkewedString(int i, std::mt19937 & rd)
{
    return BigString(NumberString(i), getSkewedNum(17, rd));
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

#if 0
    void reset_source_contents() { source_->contents_ = dest_contents(); }
#endif

    const String & destContents() const
    {
        return reader_contents;
    }


    class StringSouce : public DB::ReadBufferFromFileBase
    {
    public:
        String & contents;
        size_t read_pos;
        bool fail_after_read_partial;
        bool returned_partial;
        bool force_error;
        size_t force_error_position;
        bool force_eof;
        size_t force_eof_position;

        explicit StringSouce(String & contents_, bool fail_after_read_partial_)
            : DB::ReadBufferFromFileBase(PS::V3::Format::BLOCK_SIZE, nullptr, 0)
            , contents(contents_)
            , read_pos(0)
            , fail_after_read_partial(fail_after_read_partial_)
            , returned_partial(false)
            , force_error(false)
            , force_error_position(0)
            , force_eof(false)
            , force_eof_position(0)
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

            if (force_error)
            {
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
    // StringSource * source_;
    ReportCollector report;
    std::unique_ptr<LogWriter> writer_;
    std::unique_ptr<LogReader> reader_;
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
        writer_ = std::make_unique<LogWriter>(std::move(file_writer), /*log_num*/ log_file_num, /*recycle_log*/ recyclable_log);
        std::unique_ptr<ReadBufferFromFileBase> file_reader = std::make_unique<StringSouce>(reader_contents, /*fail_after_read_partial_*/ !allow_retry_read);
#if 0
        if (allow_retry_read_)
        {
            reader_.reset(new FragmentBufferedReader(nullptr, std::move(file_reader), &report_, true /* checksum */, log_file_num /* log_number */));
        }
        else
        {
#endif
        reader_ = std::make_unique<LogReader>(std::move(file_reader), &report, /* verify_checksum */ true, /* log_number */ log_file_num, log);
#if 0
        }
#endif
    }

    void write(const std::string & msg)
    {
        ReadBufferFromString buff(msg);
        ASSERT_NO_THROW(writer_->addRecord(buff, msg.size()));
    }

#if 0
    Slice * get_reader_contents() { return &reader_contents_; }
#endif

    size_t writtenBytes() const
    {
        return destContents().size();
    }

    std::string Read(const WALRecoveryMode wal_recovery_mode = WALRecoveryMode::TolerateCorruptedTailRecords)
    {
        if (auto [ok, scratch] = reader_->readRecord(wal_recovery_mode); ok)
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

    void forceErrorAt(size_t /*position*/)
    {
        // source_->force_error_ = true;
        // source_->force_error_position_ = position;
        FailPointHelper::enableFailPoint(::DB::FailPoints::exception_when_read_from_log);
    }

    size_t DroppedBytes() const
    {
        return report.dropped_bytes;
    }

    std::string ReportMessage() const
    {
        return report.message;
    }

#if 0
    void ForceEOF(size_t position = 0)
    {
        source_->force_eof_ = true;
        source_->force_eof_position_ = position;
    }

    void UnmarkEOF()
    {
        source_->returned_partial_ = false;
        reader_->UnmarkEOF();
    }

    bool IsEOF() { return reader_->IsEOF(); }

#endif
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
        write(NumberString(i));
    }
    for (size_t i = 0; i < num_blocks_test; i++)
    {
        auto res = Read();
        ASSERT_EQ(NumberString(i), res);
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
    ASSERT_EQ(0, DroppedBytes());
    ASSERT_EQ("", ReportMessage());
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

// Tests of all the error paths in log_reader.cc follow:

TEST_P(WALLogTest, ReadError)
{
    write("foo");
    forceErrorAt(0);
    ASSERT_EQ("EOF", Read());
    ASSERT_EQ(PS::V3::Format::BLOCK_SIZE, DroppedBytes());
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
    ASSERT_EQ(3, DroppedBytes());
    ASSERT_EQ("OK", matchError("unknown record type"));
}

TEST_P(WALLogTest, TruncatedTrailingRecordIsIgnored)
{
    write("foo");
    shrinkSize(4); // Drop all payload as well as a header byte
    ASSERT_EQ("EOF", Read());
    // Truncated last record is ignored, not treated as an error
    ASSERT_EQ(0, DroppedBytes());
    ASSERT_EQ("", ReportMessage());
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
    ASSERT_GT(DroppedBytes(), 0U);
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
        ASSERT_EQ(PS::V3::Format::BLOCK_SIZE, DroppedBytes());
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
    ASSERT_EQ(0, DroppedBytes());
    ASSERT_EQ("", ReportMessage());
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
    ASSERT_GT(DroppedBytes(), 0);
    ASSERT_EQ("OK", matchError("Corruption: truncated record body"));
}

TEST_P(WALLogTest, ChecksumMismatch)
{
    write("foooooo");
    incrementByte(0, 14);
    ASSERT_EQ("EOF", Read());
    if (!recyclable_log)
    {
        ASSERT_EQ(14, DroppedBytes());
        ASSERT_EQ("OK", matchError("checksum mismatch"));
    }
    else
    {
        ASSERT_EQ(0, DroppedBytes());
        ASSERT_EQ("", ReportMessage());
    }
}

TEST_P(WALLogTest, UnexpectedMiddleType)
{
    write("foo");
    setByte(6, static_cast<char>(recyclable_log ? Format::RecyclableMiddleType : Format::MiddleType));
    fixChecksum(0, 3, !!recyclable_log);
    ASSERT_EQ("EOF", Read());
    ASSERT_EQ(3, DroppedBytes());
    ASSERT_EQ("OK", matchError("missing start"));
}

TEST_P(WALLogTest, UnexpectedLastType)
{
    write("foo");
    setByte(6, static_cast<char>(recyclable_log ? Format::RecyclableLastType : Format::LastType));
    fixChecksum(0, 3, recyclable_log);
    ASSERT_EQ("EOF", Read());
    ASSERT_EQ(3, DroppedBytes());
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
    ASSERT_EQ(3U, DroppedBytes());
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
    ASSERT_EQ(3U, DroppedBytes());
    ASSERT_EQ("OK", matchError("partial record without end"));
}

TEST_P(WALLogTest, MissingLastIsIgnored)
{
    write(BigString("bar", PS::V3::Format::BLOCK_SIZE));
    // Remove the LAST block, including header.
    shrinkSize(14);
    ASSERT_EQ("EOF", Read());
    ASSERT_EQ("", ReportMessage());
    ASSERT_EQ(0U, DroppedBytes());
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
    ASSERT_GT(DroppedBytes(), 0);
    ASSERT_EQ("OK", matchError("Corruption: error reading trailing data"));
}

TEST_P(WALLogTest, PartialLastIsIgnored)
{
    write(BigString("bar", PS::V3::Format::BLOCK_SIZE));
    // Cause a bad record length in the LAST block.
    shrinkSize(1);
    ASSERT_EQ("EOF", Read());
    ASSERT_EQ("", ReportMessage());
    ASSERT_EQ(0, DroppedBytes());
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
    ASSERT_GT(DroppedBytes(), 0);
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
        size_t dropped = DroppedBytes();
        ASSERT_LE(dropped, 2 * PS::V3::Format::BLOCK_SIZE + 100);
        ASSERT_GE(dropped, 2 * PS::V3::Format::BLOCK_SIZE);
    }
    else
    {
        ASSERT_EQ("EOF", Read());
    }
}

#if 0
TEST_P(WALLogTest, ClearEofSingleBlock)
{
    write("foo");
    write("bar");
    int header_size = recyclable_log ? PS::V3::Format::RECYCLABLE_HEADER_SIZE : PS::V3::Format::HEADER_SIZE;
    ForceEOF(3 + header_size + 2);
    ASSERT_EQ("foo", Read());
    UnmarkEOF();
    ASSERT_EQ("bar", Read());
    ASSERT_TRUE(IsEOF());
    ASSERT_EQ("EOF", Read());
    write("xxx");
    UnmarkEOF();
    ASSERT_EQ("xxx", Read());
    ASSERT_TRUE(IsEOF());
}

TEST_P(WALLogTest, ClearEofMultiBlock)
{
    size_t num_full_blocks = 5;
    int header_size = recyclable_log ? PS::V3::Format::RECYCLABLE_HEADER_SIZE : PS::V3::Format::HEADER_SIZE;
    size_t n = (PS::V3::Format::BLOCK_SIZE - header_size) * num_full_blocks + 25;
    write(BigString("foo", n));
    write(BigString("bar", n));
    ForceEOF(n + num_full_blocks * header_size + header_size + 3);
    ASSERT_EQ(BigString("foo", n), Read());
    ASSERT_TRUE(IsEOF());
    UnmarkEOF();
    ASSERT_EQ(BigString("bar", n), Read());
    ASSERT_TRUE(IsEOF());
    write(BigString("xxx", n));
    UnmarkEOF();
    ASSERT_EQ(BigString("xxx", n), Read());
    ASSERT_TRUE(IsEOF());
}
#endif

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

#if 0
class RetriableWALLogTest : public ::testing::TestWithParam<int>
{
private:
    class ReportCollector : public Reader::Reporter
    {
    public:
        size_t dropped_bytes_;
        std::string message_;

        ReportCollector()
            : dropped_bytes_(0)
        {}
        void Corruption(size_t bytes, const Status & status) override
        {
            dropped_bytes_ += bytes;
            message_.append(status.ToString());
        }
    };

    Slice contents_;
    test::StringSink * sink_;
    std::unique_ptr<Writer> log_writer_;
    Env * env_;
    const std::string test_dir_;
    const std::string log_file_;
    std::unique_ptr<WritableFileWriter> writer_;
    std::unique_ptr<SequentialFileReader> reader_;
    ReportCollector report_;
    std::unique_ptr<FragmentBufferedReader> log_reader_;

public:
    RetriableWALLogTest()
        : contents_()
        , sink_(new test::StringSink(&contents_))
        , log_writer_(nullptr)
        , env_(Env::Default())
        , test_dir_(test::PerThreadDBPath("retriable_log_test"))
        , log_file_(test_dir_ + "/log")
        , writer_(nullptr)
        , reader_(nullptr)
        , log_reader_(nullptr)
    {
        std::unique_ptr<FSWritableFile> sink_holder(sink_);
        std::unique_ptr<WritableFileWriter> wfw(new WritableFileWriter(
            std::move(sink_holder),
            "" /* file name */,
            FileOptions()));
        log_writer_.reset(new Writer(std::move(wfw), 123, GetParam()));
    }

    Status SetupTestEnv()
    {
        Status s;
        FileOptions fopts;
        auto fs = env_->GetFileSystem();
        s = fs->CreateDirIfMissing(test_dir_, IOOptions(), nullptr);
        std::unique_ptr<FSWritableFile> writable_file;
        if (s.ok())
        {
            s = fs->NewWritableFile(log_file_, fopts, &writable_file, nullptr);
        }
        if (s.ok())
        {
            writer_.reset(
                new WritableFileWriter(std::move(writable_file), log_file_, fopts));
            EXPECT_NE(writer_, nullptr);
        }
        std::unique_ptr<FSSequentialFile> seq_file;
        if (s.ok())
        {
            s = fs->NewSequentialFile(log_file_, fopts, &seq_file, nullptr);
        }
        if (s.ok())
        {
            reader_.reset(new SequentialFileReader(std::move(seq_file), log_file_));
            EXPECT_NE(reader_, nullptr);
            log_reader_.reset(new FragmentBufferedReader(
                nullptr,
                std::move(reader_),
                &report_,
                true /* checksum */,
                123 /* log_number */));
            EXPECT_NE(log_reader_, nullptr);
        }
        return s;
    }

    std::string contents() { return sink_->contents_; }

    void Encode(const std::string & msg)
    {
        ASSERT_OK(log_writer_->AddRecord(Slice(msg)));
    }

    void write(const Slice & data)
    {
        ASSERT_OK(writer_->Append(data));
        ASSERT_OK(writer_->Sync(true));
    }

    bool TryRead(std::string * result)
    {
        assert(result != nullptr);
        result->clear();
        std::string scratch;
        Slice record;
        bool r = log_reader_->ReadRecord(&record, &scratch);
        if (r)
        {
            result->assign(record.data(), record.size());
            return true;
        }
        else
        {
            return false;
        }
    }
};

TEST_P(RetriableWALLogTest, TailLog_PartialHeader)
{
    ASSERT_OK(SetupTestEnv());
    std::vector<int> remaining_bytes_in_last_record;
    size_t header_size = GetParam() ? PS::V3::Format::RECYCLABLE_HEADER_SIZE : PS::V3::Format::HEADER_SIZE;
    bool eof = false;
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->LoadDependency(
        {{"RetriableWALLogTest::TailLog:AfterPart1",
          "RetriableWALLogTest::TailLog:BeforeReadRecord"},
         {"FragmentBufferedLogReader::TryReadMore:FirstEOF",
          "RetriableWALLogTest::TailLog:BeforePart2"}});
    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->SetCallBack(
        "FragmentBufferedLogReader::TryReadMore:FirstEOF",
        [&](void * /*arg*/) { eof = true; });
    SyncPoint::GetInstance()->EnableProcessing();

    size_t delta = header_size - 1;
    port::Thread log_writer_thread([&]() {
        size_t old_sz = contents().size();
        Encode("foo");
        size_t new_sz = contents().size();
        std::string part1 = contents().substr(old_sz, delta);
        std::string part2 = contents().substr(old_sz + delta, new_sz - old_sz - delta);
        write(Slice(part1));
        TEST_SYNC_POINT("RetriableWALLogTest::TailLog:AfterPart1");
        TEST_SYNC_POINT("RetriableWALLogTest::TailLog:BeforePart2");
        write(Slice(part2));
    });

    std::string record;
    port::Thread log_reader_thread([&]() {
        TEST_SYNC_POINT("RetriableWALLogTest::TailLog:BeforeReadRecord");
        while (!TryRead(&record))
        {
        }
    });
    log_reader_thread.join();
    log_writer_thread.join();
    ASSERT_EQ("foo", record);
    ASSERT_TRUE(eof);
}

TEST_P(RetriableWALLogTest, TailLog_FullHeader)
{
    ASSERT_OK(SetupTestEnv());
    std::vector<int> remaining_bytes_in_last_record;
    size_t header_size = GetParam() ? PS::V3::Format::RECYCLABLE_HEADER_SIZE : PS::V3::Format::HEADER_SIZE;
    bool eof = false;
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->LoadDependency(
        {{"RetriableWALLogTest::TailLog:AfterPart1",
          "RetriableWALLogTest::TailLog:BeforeReadRecord"},
         {"FragmentBufferedLogReader::TryReadMore:FirstEOF",
          "RetriableWALLogTest::TailLog:BeforePart2"}});
    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->SetCallBack(
        "FragmentBufferedLogReader::TryReadMore:FirstEOF",
        [&](void * /*arg*/) { eof = true; });
    SyncPoint::GetInstance()->EnableProcessing();

    size_t delta = header_size + 1;
    port::Thread log_writer_thread([&]() {
        size_t old_sz = contents().size();
        Encode("foo");
        size_t new_sz = contents().size();
        std::string part1 = contents().substr(old_sz, delta);
        std::string part2 = contents().substr(old_sz + delta, new_sz - old_sz - delta);
        write(Slice(part1));
        TEST_SYNC_POINT("RetriableWALLogTest::TailLog:AfterPart1");
        TEST_SYNC_POINT("RetriableWALLogTest::TailLog:BeforePart2");
        write(Slice(part2));
        ASSERT_TRUE(eof);
    });

    std::string record;
    port::Thread log_reader_thread([&]() {
        TEST_SYNC_POINT("RetriableWALLogTest::TailLog:BeforeReadRecord");
        while (!TryRead(&record))
        {
        }
    });
    log_reader_thread.join();
    log_writer_thread.join();
    ASSERT_EQ("foo", record);
}

TEST_P(RetriableWALLogTest, NonBlockingReadFullRecord)
{
    // Clear all sync point callbacks even if this test does not use sync point.
    // It is necessary, otherwise the execute of this test may hit a sync point
    // with which a callback is registered. The registered callback may access
    // some dead variable, causing segfault.
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_OK(SetupTestEnv());
    size_t header_size = GetParam() ? PS::V3::Format::RECYCLABLE_HEADER_SIZE : PS::V3::Format::HEADER_SIZE;
    size_t delta = header_size - 1;
    size_t old_sz = contents().size();
    Encode("foo-bar");
    size_t new_sz = contents().size();
    std::string part1 = contents().substr(old_sz, delta);
    std::string part2 = contents().substr(old_sz + delta, new_sz - old_sz - delta);
    write(Slice(part1));
    std::string record;
    ASSERT_FALSE(TryRead(&record));
    ASSERT_TRUE(record.empty());
    write(Slice(part2));
    ASSERT_TRUE(TryRead(&record));
    ASSERT_EQ("foo-bar", record);
}

INSTANTIATE_TEST_CASE_P(bool, RetriableWALLogTest, ::testing::Values(0, 2));
#endif

} // namespace DB::PS::V3::tests
