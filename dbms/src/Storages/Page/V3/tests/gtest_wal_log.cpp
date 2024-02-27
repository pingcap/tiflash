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

#include <Common/FailPoint.h>
#include <Common/RedactHelpers.h>
#include <Core/Defines.h>
#include <IO/BaseFile/WriteReadableFile.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Buffer/WriteBuffer.h>
#include <IO/Buffer/WriteBufferFromFile.h>
#include <IO/Buffer/WriteBufferFromFileBase.h>
#include <IO/FileProvider/EncryptionPath.h>
#include <IO/FileProvider/ReadBufferFromRandomAccessFileBuilder.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/PageUtil.h>
#include <Storages/Page/V3/LogFile/LogFormat.h>
#include <Storages/Page/V3/LogFile/LogReader.h>
#include <Storages/Page/V3/LogFile/LogWriter.h>
#include <Storages/Page/V3/WAL/WALReader.h>
#include <Storages/Page/V3/WALStore.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/logger_useful.h>
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
static String repeatedString(const String & partial_string, size_t n)
{
    String result;
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
static String randomSkewedString(int i, std::mt19937 & rd)
{
    return repeatedString(DB::toString(i), getSkewedNum(17, rd));
}

// Param type is tuple<int, bool>
// get<0>(tuple): non-zero if recycling log, zero if regular log
// get<1>(tuple): true if allow retry after read EOF, false otherwise
class LogFileRWTest : public ::testing::TestWithParam<std::tuple<bool, bool>>
{
private:
    class ReportCollector : public LogReader::Reporter
    {
    public:
        size_t dropped_bytes;
        String message{};

        ReportCollector()
            : dropped_bytes(0)
        {}
        void corruption(size_t bytes, const String & msg) override
        {
            dropped_bytes += bytes;
            message.append(msg);
        }
    };

    ReportCollector report;
    std::unique_ptr<LogWriter> writer{};
    std::unique_ptr<LogReader> reader{};
    LoggerPtr log{};

protected:
    String path{};
    String file_name{};
    FileProviderPtr provider;
    WriteReadableFilePtr wr_file;

    bool recyclable_log;
    bool allow_retry_read;
    const int log_file_num = 123;

public:
    LogFileRWTest()
        : log(Logger::get())
        , recyclable_log(std::get<0>(GetParam()))
        , allow_retry_read(std::get<1>(GetParam()))
    {
        provider = TiFlashTestEnv::getDefaultFileProvider();
        path = TiFlashTestEnv::getTemporaryPath("LogFileRWTest");
        DB::tests::TiFlashTestEnv::tryRemovePath(path);

        Poco::File file(path);
        if (!file.exists())
        {
            file.createDirectories();
        }

        file_name = path + "/log_0";

        writer = std::make_unique<LogWriter>(
            file_name,
            provider,
            /*log_num*/ log_file_num,
            /*recycle_log*/ recyclable_log);
        resetReader();


        wr_file = provider->newWriteReadableFile(
            file_name,
            EncryptionPath{file_name, ""},
            false,
            /*create_new_encryption_info_*/ false);
    }

    std::unique_ptr<LogReader> getNewReader(
        const WALRecoveryMode wal_recovery_mode = WALRecoveryMode::TolerateCorruptedTailRecords,
        size_t log_num = 0)
    {
        auto read_buf = ReadBufferFromRandomAccessFileBuilder::buildPtr(
            provider,
            file_name,
            EncryptionPath{file_name, ""},
            Format::BLOCK_SIZE // Must be `Format::BLOCK_SIZE`
        );
        return std::make_unique<LogReader>(
            std::move(read_buf),
            &report,
            /* verify_checksum */ true,
            /* log_number */ log_num,
            wal_recovery_mode);
    }

    void resetReader(const WALRecoveryMode wal_recovery_mode = WALRecoveryMode::TolerateCorruptedTailRecords)
    {
        reader = getNewReader(wal_recovery_mode, log_file_num);
    }

    void write(const std::string & msg)
    {
        ReadBufferFromString buff(msg);

        ASSERT_NO_THROW(writer->addRecord(buff, msg.size()));
    }

    size_t writtenBytes() const
    {
        Poco::File file_in_disk(file_name);
        return file_in_disk.getSize();
    }

    String read()
    {
        if (auto [ok, scratch] = reader->readRecord(); ok)
            return scratch;
        return "EOF";
    }

    /// Some methods to break to written bytes

    void incrementByte(int offset, char delta)
    {
        char old_one[1] = "";
        PageUtil::readFile(wr_file, offset, old_one, 1, nullptr);
        old_one[0] += delta;
        PageUtil::writeFile(wr_file, offset, old_one, 1, nullptr);
    }

    void setByte(int offset, char new_byte) { PageUtil::writeFile(wr_file, offset, &new_byte, 1, nullptr); }

    void shrinkSize(int bytes) { PageUtil::ftruncateFile(wr_file, writtenBytes() - bytes); }

    void fixChecksum(int header_offset, int payload_len, bool recyclable)
    {
        // Compute crc of type/len/data
        int header_size = recyclable ? Format::RECYCLABLE_HEADER_SIZE : Format::HEADER_SIZE;
        Format::ChecksumClass digest;

        size_t crc_buff_size = header_size - Format::CHECKSUM_START_OFFSET + payload_len;
        char crc_buff[crc_buff_size];
        PageUtil::readFile(wr_file, header_offset + Format::CHECKSUM_START_OFFSET, crc_buff, crc_buff_size, nullptr);

        digest.update(crc_buff, crc_buff_size);

        auto checksum = digest.checksum();
        PageUtil::writeFile(wr_file, header_offset, reinterpret_cast<char *>(&checksum), sizeof(checksum), nullptr);
    }

    /// Some methods to check the error reporter

    size_t droppedBytes() const { return report.dropped_bytes; }

    String reportMessage() const { return report.message; }

    // Returns OK iff recorded error message contains "msg"
    String matchError(const std::string & msg) const
    {
        if (report.message.find(msg) == std::string::npos)
            return report.message;
        return "OK";
    }
};

TEST_P(LogFileRWTest, Empty)
{
    ASSERT_EQ("EOF", read());
}

TEST_P(LogFileRWTest, ReadWrite)
try
{
    write("foo");
    write("bar");
    write("");
    write("xxxx");
    resetReader();
    ASSERT_EQ("foo", read());
    ASSERT_EQ("bar", read());
    ASSERT_EQ("", read());
    ASSERT_EQ("xxxx", read());
    ASSERT_EQ("EOF", read());
    ASSERT_EQ("EOF", read()); // Make sure reads at eof work
}
CATCH

TEST_P(LogFileRWTest, BlockBoundary)
{
    const auto big_str = repeatedString("A", PS::V3::Format::BLOCK_SIZE - Format::HEADER_SIZE - 4);
    write(big_str);
    write("small");
    resetReader();
    ASSERT_EQ(big_str, read());
    ASSERT_EQ("small", read());
    ASSERT_EQ("EOF", read());
}

TEST_P(LogFileRWTest, BlockBoundaryEmpty)
{
    const auto big_str = repeatedString("A", PS::V3::Format::BLOCK_SIZE - Format::HEADER_SIZE - 4);
    write(big_str);
    write("");
    resetReader();
    ASSERT_EQ(big_str, read());
    ASSERT_EQ("", read());
    ASSERT_EQ("EOF", read());
}

TEST_P(LogFileRWTest, ManyBlocks)
{
    const size_t num_blocks_test = 100000;
    for (size_t i = 0; i < num_blocks_test; i++)
    {
        write(DB::toString(i));
    }
    resetReader();
    for (size_t i = 0; i < num_blocks_test; i++)
    {
        auto res = read();
        ASSERT_EQ(DB::toString(i), res);
    }
    ASSERT_EQ("EOF", read());
}

TEST_P(LogFileRWTest, Fragmentation)
{
    write("small");
    write(repeatedString("medium", 50000));
    write(repeatedString("large", 100000));
    resetReader();
    ASSERT_EQ("small", read());
    ASSERT_EQ(repeatedString("medium", 50000), read());
    ASSERT_EQ(repeatedString("large", 100000), read());
    ASSERT_EQ("EOF", read());
}

// This test may take a lot of time
TEST_P(LogFileRWTest, DISABLED_DifferentPayloadSize)
{
    for (size_t i = 0; i < 40000; i += 1)
    {
        write(repeatedString("a", i));
    }
}

TEST_P(LogFileRWTest, MarginalTrailer)
{
    // Make a trailer that is exactly the same length as an empty record.
    int header_size = recyclable_log ? PS::V3::Format::RECYCLABLE_HEADER_SIZE : PS::V3::Format::HEADER_SIZE;
    const int n = PS::V3::Format::BLOCK_SIZE - 2 * header_size;
    write(repeatedString("foo", n));
    ASSERT_EQ(static_cast<size_t>(PS::V3::Format::BLOCK_SIZE - header_size), writtenBytes());
    write("");
    write("bar");
    resetReader();
    ASSERT_EQ(repeatedString("foo", n), read());
    ASSERT_EQ("", read());
    ASSERT_EQ("bar", read());
    ASSERT_EQ("EOF", read());
}

TEST_P(LogFileRWTest, MarginalTrailer2)
{
    // Make a trailer that is exactly the same length as an empty record.
    int header_size = recyclable_log ? PS::V3::Format::RECYCLABLE_HEADER_SIZE : PS::V3::Format::HEADER_SIZE;
    const int n = PS::V3::Format::BLOCK_SIZE - 2 * header_size;
    write(repeatedString("foo", n));
    ASSERT_EQ((unsigned int)(PS::V3::Format::BLOCK_SIZE - header_size), writtenBytes());
    write("bar");
    resetReader();
    ASSERT_EQ(repeatedString("foo", n), read());
    ASSERT_EQ("bar", read());
    ASSERT_EQ("EOF", read());
    ASSERT_EQ(0, droppedBytes());
    ASSERT_EQ("", reportMessage());
}

TEST_P(LogFileRWTest, ShortTrailer)
{
    int header_size = recyclable_log ? PS::V3::Format::RECYCLABLE_HEADER_SIZE : PS::V3::Format::HEADER_SIZE;
    const int n = PS::V3::Format::BLOCK_SIZE - 2 * header_size + 4;
    write(repeatedString("foo", n));
    ASSERT_EQ((unsigned int)(PS::V3::Format::BLOCK_SIZE - header_size + 4), writtenBytes());
    write("");
    write("bar");
    resetReader();
    ASSERT_EQ(repeatedString("foo", n), read());
    ASSERT_EQ("", read());
    ASSERT_EQ("bar", read());
    ASSERT_EQ("EOF", read());
}

TEST_P(LogFileRWTest, AlignedEOF)
{
    int header_size = recyclable_log ? PS::V3::Format::RECYCLABLE_HEADER_SIZE : PS::V3::Format::HEADER_SIZE;
    const int n = PS::V3::Format::BLOCK_SIZE - 2 * header_size + 4;
    write(repeatedString("foo", n));
    ASSERT_EQ((unsigned int)(PS::V3::Format::BLOCK_SIZE - header_size + 4), writtenBytes());
    resetReader();
    ASSERT_EQ(repeatedString("foo", n), read());
    ASSERT_EQ("EOF", read());
}

TEST_P(LogFileRWTest, RandomRead)
{
    constexpr int n = 500;
    constexpr int rand_seed = 301;
    std::mt19937 write_rd(rand_seed);
    for (int i = 0; i < n; i++)
    {
        write(randomSkewedString(i, write_rd));
    }
    resetReader();
    std::mt19937 read_rd(rand_seed);
    for (int i = 0; i < n; i++)
    {
        ASSERT_EQ(randomSkewedString(i, read_rd), read());
    }
    ASSERT_EQ("EOF", read());
}

/// Tests of all the error paths in LogReader.cpp follow:

TEST_P(LogFileRWTest, ReadError)
{
    write("foo");
    resetReader();
    FailPointHelper::enableFailPoint(::DB::FailPoints::exception_when_read_from_log);
    ASSERT_EQ("EOF", read());
    ASSERT_EQ(PS::V3::Format::BLOCK_SIZE, droppedBytes());
    ASSERT_EQ("OK", matchError("exception_when_read_from_log"));
}

TEST_P(LogFileRWTest, BadRecordType)
{
    write("foo");
    resetReader();
    // Type is stored in header[`CHECKSUM_START_OFFSET`], break the type
    incrementByte(Format::CHECKSUM_START_OFFSET, 100);
    // Meeting a unknown type, consider its header size as `Format::HEADER_SIZE`
    fixChecksum(0, 3, /*recyclable*/ false);
    // Can not successfully read the BadRecord, and get dropped bytes, message reported
    ASSERT_EQ("EOF", read());
    ASSERT_EQ(3, droppedBytes());
    ASSERT_EQ("OK", matchError("unknown record type"));
}

TEST_P(LogFileRWTest, TruncatedTrailingRecordIsIgnored)
{
    write("foo");
    shrinkSize(3 + sizeof(Format::MaxRecordType)); // Drop all payload as well as a header byte
    resetReader();
    ASSERT_EQ("EOF", read());
    // Truncated last record is ignored, not treated as an error
    ASSERT_EQ(0, droppedBytes());
    ASSERT_EQ("", reportMessage());
}

TEST_P(LogFileRWTest, TruncatedTrailingRecordIsNotIgnored)
{
    if (allow_retry_read)
    {
        // If read retry is allowed, then truncated trailing record should not
        // raise an error.
        return;
    }

    write("foo");
    shrinkSize(3 + sizeof(Format::MaxRecordType)); // Drop all payload as well as a header byte
    resetReader(WALRecoveryMode::AbsoluteConsistency);
    ASSERT_EQ("EOF", read());
    // Truncated last record is ignored, not treated as an error
    ASSERT_GT(droppedBytes(), 0);
    ASSERT_EQ("OK", matchError("Corruption: truncated header"));
}

TEST_P(LogFileRWTest, BadLength)
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
    write(repeatedString("bar", payload_size));
    write("foo");
    resetReader();
    // Least significant size byte is stored in header[SizePos].
    incrementByte(Format::CHECKSUM_FIELD_SIZE, 1);
    if (!recyclable_log)
    {
        ASSERT_EQ("foo", read());
        ASSERT_EQ(PS::V3::Format::BLOCK_SIZE, droppedBytes());
        ASSERT_EQ("OK", matchError("bad record length"));
    }
    else
    {
        ASSERT_EQ("EOF", read());
    }
}

TEST_P(LogFileRWTest, BadLengthAtEndIsIgnored)
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
    resetReader();
    ASSERT_EQ("EOF", read());
    ASSERT_EQ(0, droppedBytes());
    ASSERT_EQ("", reportMessage());
}

TEST_P(LogFileRWTest, BadLengthAtEndIsNotIgnored)
{
    if (allow_retry_read)
    {
        // If read retry is allowed, then we should not raise an error when the
        // record length specified in header is longer than data currently
        // available. It's possible that the body of the record is not written yet.
        return;
    }
    resetReader(WALRecoveryMode::AbsoluteConsistency);
    write("foo");
    shrinkSize(1);
    ASSERT_EQ("EOF", read());
    ASSERT_GT(droppedBytes(), 0);
    ASSERT_EQ("OK", matchError("Corruption: truncated record body"));
}

TEST_P(LogFileRWTest, ChecksumMismatch)
{
    write("foooooo");
    incrementByte(0, Format::HEADER_SIZE + 7);
    ASSERT_EQ("EOF", read());
    if (!recyclable_log)
    {
        ASSERT_EQ(Format::HEADER_SIZE + 7, droppedBytes());
        ASSERT_EQ("OK", matchError("checksum mismatch"));
    }
    else
    {
        ASSERT_EQ(0, droppedBytes());
        ASSERT_EQ("", reportMessage());
    }
}

TEST_P(LogFileRWTest, UnexpectedMiddleType)
{
    write("foo");
    setByte(
        Format::CHECKSUM_START_OFFSET,
        static_cast<char>(recyclable_log ? Format::RecyclableMiddleType : Format::MiddleType));
    fixChecksum(0, 3, recyclable_log);
    ASSERT_EQ("EOF", read());
    ASSERT_EQ(3, droppedBytes());
    ASSERT_EQ("OK", matchError("missing start"));
}

TEST_P(LogFileRWTest, UnexpectedLastType)
{
    write("foo");
    setByte(
        Format::CHECKSUM_START_OFFSET,
        static_cast<char>(recyclable_log ? Format::RecyclableLastType : Format::LastType));
    fixChecksum(0, 3, recyclable_log);
    ASSERT_EQ("EOF", read());
    ASSERT_EQ(3, droppedBytes());
    ASSERT_EQ("OK", matchError("missing start"));
}

TEST_P(LogFileRWTest, UnexpectedFullType)
{
    write("foo");
    write("bar");
    setByte(
        Format::CHECKSUM_START_OFFSET,
        static_cast<char>(recyclable_log ? Format::RecyclableFirstType : Format::FirstType));
    fixChecksum(0, 3, recyclable_log);
    ASSERT_EQ("bar", read());
    ASSERT_EQ("EOF", read());
    ASSERT_EQ(3, droppedBytes());
    ASSERT_EQ("OK", matchError("partial record without end"));
}

TEST_P(LogFileRWTest, UnexpectedFirstType)
{
    write("foo");
    write(repeatedString("bar", 100000));
    setByte(
        Format::CHECKSUM_START_OFFSET,
        static_cast<char>(recyclable_log ? Format::RecyclableFirstType : Format::FirstType));
    fixChecksum(0, 3, recyclable_log);
    ASSERT_EQ(repeatedString("bar", 100000), read());
    ASSERT_EQ("EOF", read());
    ASSERT_EQ(3, droppedBytes());
    ASSERT_EQ("OK", matchError("partial record without end"));
}

TEST_P(LogFileRWTest, MissingLastIsIgnored)
{
    write(repeatedString("bar", PS::V3::Format::BLOCK_SIZE));
    // Remove the LAST block, including header.
    shrinkSize(2 * (recyclable_log ? Format::RECYCLABLE_HEADER_SIZE : Format::HEADER_SIZE));
    ASSERT_EQ("EOF", read());
    ASSERT_EQ("", reportMessage());
    ASSERT_EQ(0, droppedBytes());
}

TEST_P(LogFileRWTest, MissingLastIsNotIgnored)
{
    if (allow_retry_read)
    {
        // If read retry is allowed, then truncated trailing record should not
        // raise an error.
        return;
    }
    resetReader(WALRecoveryMode::AbsoluteConsistency);
    write(repeatedString("bar", PS::V3::Format::BLOCK_SIZE));
    // Remove the LAST block, including header.
    shrinkSize(2 * (recyclable_log ? Format::RECYCLABLE_HEADER_SIZE : Format::HEADER_SIZE));
    ASSERT_EQ("EOF", read());
    ASSERT_GT(droppedBytes(), 0);
    ASSERT_EQ("OK", matchError("Corruption: error reading trailing data"));
}

TEST_P(LogFileRWTest, PartialLastIsIgnored)
{
    write(repeatedString("bar", PS::V3::Format::BLOCK_SIZE));
    // Cause a bad record length in the LAST block.
    shrinkSize(1);
    ASSERT_EQ("EOF", read());
    ASSERT_EQ("", reportMessage());
    ASSERT_EQ(0, droppedBytes());
}

TEST_P(LogFileRWTest, PartialLastIsNotIgnored)
{
    if (allow_retry_read)
    {
        // If read retry is allowed, then truncated trailing record should not
        // raise an error.
        return;
    }
    resetReader(WALRecoveryMode::AbsoluteConsistency);
    write(repeatedString("bar", PS::V3::Format::BLOCK_SIZE));
    // Cause a bad record length in the LAST block.
    shrinkSize(1);
    ASSERT_EQ("EOF", read());
    ASSERT_GT(droppedBytes(), 0);
    ASSERT_EQ("OK", matchError("Corruption: truncated record body"));
}

TEST_P(LogFileRWTest, ErrorJoinsRecords)
{
    // Consider two fragmented records:
    //    first(R1) last(R1) first(R2) last(R2)
    // where the middle two fragments disappear.  We do not want
    // first(R1),last(R2) to get joined and returned as a valid record.

    // write records that span two blocks
    write(repeatedString("foo", PS::V3::Format::BLOCK_SIZE));
    write(repeatedString("bar", PS::V3::Format::BLOCK_SIZE));
    write("correct");

    // Wipe the middle block
    for (unsigned int offset = PS::V3::Format::BLOCK_SIZE; offset < 2 * PS::V3::Format::BLOCK_SIZE; offset++)
    {
        setByte(offset, 'x');
    }

    if (!recyclable_log)
    {
        ASSERT_EQ("correct", read());
        ASSERT_EQ("EOF", read());
        size_t dropped = droppedBytes();
        ASSERT_LE(dropped, 2 * PS::V3::Format::BLOCK_SIZE + 100);
        ASSERT_GE(dropped, 2 * PS::V3::Format::BLOCK_SIZE);
    }
    else
    {
        ASSERT_EQ("EOF", read());
    }
}

TEST_P(LogFileRWTest, Recycle)
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
    while (writtenBytes() < PS::V3::Format::BLOCK_SIZE * 2)
    {
        write("xxxxxxxxxxxxxxxx");
    }
    const size_t content_size_before_overwrite = writtenBytes();

    // Overwrite some record with same log file number
    std::unique_ptr<LogWriter> recycle_writer = std::make_unique<LogWriter>(
        file_name,
        provider,
        /*log_num*/ log_file_num,
        /*recycle_log*/ recyclable_log);

    String text_to_write = "foooo";
    ReadBufferFromString foo(text_to_write);
    recycle_writer->addRecord(foo, text_to_write.size());
    text_to_write = "bar";
    ReadBufferFromString bar(text_to_write);
    recycle_writer->addRecord(bar, text_to_write.size());

    // Check that we should only read new records overwrited (with the same log number)
    ASSERT_GE(writtenBytes(), PS::V3::Format::BLOCK_SIZE * 2);
    ASSERT_EQ(writtenBytes(), content_size_before_overwrite);
    ASSERT_EQ("foooo", read());
    ASSERT_EQ("bar", read());
    ASSERT_EQ("EOF", read());
}

TEST_P(LogFileRWTest, RecycleWithAnotherLogNum)
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
    while (writtenBytes() < PS::V3::Format::BLOCK_SIZE * 2)
    {
        write("xxxxxxxxxxxxxxxx");
    }
    const size_t content_size_before_overwrite = writtenBytes();

    // Overwrite some record with another log file number
    size_t overwrite_log_num = log_file_num + 1;

    std::unique_ptr<LogWriter> recycle_writer = std::make_unique<LogWriter>(
        file_name,
        provider,
        /*log_num*/ overwrite_log_num,
        /*recycle_log*/ recyclable_log);

    String text_to_write = "foooo";
    ReadBufferFromString foo(text_to_write);
    recycle_writer->addRecord(foo, text_to_write.size());
    text_to_write = "bar";
    ReadBufferFromString bar(text_to_write);
    recycle_writer->addRecord(bar, text_to_write.size());

    ASSERT_GE(writtenBytes(), PS::V3::Format::BLOCK_SIZE * 2);
    ASSERT_EQ(writtenBytes(), content_size_before_overwrite);
    // read with old log number
    ASSERT_EQ("EOF", read());

    // read with new log number
    auto new_log_reader = getNewReader(WALRecoveryMode::TolerateCorruptedTailRecords, overwrite_log_num);
    auto read_from_new = [&new_log_reader]() -> String {
        if (auto [ok, scratch] = new_log_reader->readRecord(); ok)
            return scratch;
        return "EOF";
    };
    ASSERT_EQ("foooo", read_from_new());
    ASSERT_EQ("bar", read_from_new());
    ASSERT_EQ("EOF", read_from_new());
}

TEST_P(LogFileRWTest, RecycleWithSameBoundaryLogNum)
{
    if (!recyclable_log)
    {
        return; // test is only valid for recycled logs
    }
    write("foo");
    write("bar");
    size_t boundary = writtenBytes();
    write("baz");
    write("bif");
    write("blitz");
    size_t num_writes_stuff = 0;
    while (writtenBytes() < PS::V3::Format::BLOCK_SIZE * 2)
    {
        write("xxxxxxxxxxxxxxxx");
        num_writes_stuff++;
    }
    const size_t content_size_before_overwrite = writtenBytes();

    // Overwrite some record with same log file number

    std::unique_ptr<LogWriter> recycle_writer = std::make_unique<LogWriter>(
        file_name,
        provider,
        /*log_num*/ log_file_num,
        /*recycle_log*/ recyclable_log);
    String text_to_write = repeatedString("A", boundary - PS::V3::Format::RECYCLABLE_HEADER_SIZE);
    ReadBufferFromString foo(text_to_write);
    recycle_writer->addRecord(foo, text_to_write.size());

    ASSERT_GE(writtenBytes(), PS::V3::Format::BLOCK_SIZE * 2);
    ASSERT_EQ(writtenBytes(), content_size_before_overwrite);
    // read with old log number
    ASSERT_EQ(text_to_write, read());
    ASSERT_EQ("baz", read());
    ASSERT_EQ("bif", read());
    ASSERT_EQ("blitz", read());
    while (num_writes_stuff--)
    {
        ASSERT_EQ("xxxxxxxxxxxxxxxx", read());
    }
    ASSERT_EQ("EOF", read());
}

INSTANTIATE_TEST_CASE_P(
    Recycle_AllowRetryRead,
    LogFileRWTest,
    ::testing::Combine(::testing::Bool(), ::testing::Bool()),
    [](const ::testing::TestParamInfo<LogFileRWTest::ParamType> & info) -> String {
        const auto [recycle_log, allow_retry_read] = info.param;
        return fmt::format("{}_{}", recycle_log, allow_retry_read);
    });

TEST(LogFileRWTest2, ManuallySync)
{
    auto provider = TiFlashTestEnv::getDefaultFileProvider();
    auto path = TiFlashTestEnv::getTemporaryPath("LogFileRWTest2");
    DB::tests::TiFlashTestEnv::tryRemovePath(path);

    Poco::File file(path);
    if (!file.exists())
    {
        file.createDirectories();
    }

    auto file_name = path + "/log_0";
    auto payload = repeatedString("medium", 50000);
    Format::LogNumberType log_num = 30;

    auto writer
        = std::make_unique<LogWriter>(file_name, provider, log_num, /* recycle_log */ true, /* manual_flush */ true);
    {
        ReadBufferFromString buff(payload);
        ASSERT_NO_THROW(writer->addRecord(buff, payload.size()));
    }
    {
        ReadBufferFromString buff(payload);
        ASSERT_NO_THROW(writer->addRecord(buff, payload.size()));
    }
    writer->sync();

    auto read_buf = ReadBufferFromRandomAccessFileBuilder::buildPtr(
        provider,
        file_name,
        EncryptionPath{file_name, ""},
        Format::BLOCK_SIZE // Must be `Format::BLOCK_SIZE`
    );

    DB::PS::V3::ReportCollector reporter;
    auto reader = std::make_unique<LogReader>(
        std::move(read_buf),
        &reporter,
        /* verify_checksum */ true,
        log_num,
        WALRecoveryMode::PointInTimeRecovery);
    {
        auto [ok, scratch] = reader->readRecord();
        ASSERT_TRUE(ok);
        ASSERT_EQ(scratch, payload);
    }
    {
        auto [ok, scratch] = reader->readRecord();
        ASSERT_TRUE(ok);
        ASSERT_EQ(scratch, payload);
    }
}
} // namespace DB::PS::V3::tests
