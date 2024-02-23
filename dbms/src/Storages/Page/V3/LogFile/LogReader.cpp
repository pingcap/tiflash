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

#include <Common/Checksum.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <IO/Buffer/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Storages/Page/V3/LogFile/LogFormat.h>
#include <Storages/Page/V3/LogFile/LogReader.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

#include <type_traits>

namespace DB::FailPoints
{
extern const char exception_when_read_from_log[];
}

namespace DB::PS::V3
{
LogReader::LogReader(
    std::unique_ptr<ReadBufferFromFileBase> && file_,
    Reporter * reporter_,
    bool verify_checksum_,
    Format::LogNumberType log_num_,
    WALRecoveryMode recovery_mode_)
    : verify_checksum(verify_checksum_)
    , recycled(false)
    , is_last_block(false)
    , eof(false)
    , read_error(false)
    , recovery_mode(recovery_mode_)
    , eof_offset(0)
    , file(std::move(file_))
    , buffer(file->buffer().begin(), file->buffer().size())
    , reporter(reporter_)
    , end_of_buffer_offset(0)
    , log_number(log_num_)
{
    // Must be `BLOCK_SIZE`, or we can not ensure the correctness of reading.
    assert(file->internalBuffer().size() == Format::BLOCK_SIZE);
}

LogReader::~LogReader() = default;

// State Transition Table
// +----------------------+---------------------+---------------------+-----------------+--------------------+
// | State/               | Begin read          | In fragmented       | End with        | End with           |
// | The type read        |                     | record              | record returned | no record returned |
// +======================+=====================+=====================+=================+====================+
// | FullType             | End with            | Report corruption;  | -               | -                  |
// | RecyclableFullType   | record returned     | End with            |                 |                    |
// |                      |                     | no record read      |                 |                    |
// +----------------------+---------------------+---------------------+-----------------+--------------------+
// | FirstType            | Append into buffer; | Report corruption;  | -               | -                  |
// | RecyclableFirstType  | In fragmented       | End with            |                 |                    |
// |                      | record              | no record read      |                 |                    |
// +----------------------+---------------------+---------------------+-----------------+--------------------+
// | MiddleType           | Report corruption;  | Append into buffer; | -               | -                  |
// | RecyclableMiddleType | End with            | In fragmented       |                 |                    |
// |                      | no record read      | record              |                 |                    |
// +----------------------+---------------------+---------------------+-----------------+--------------------+
// | LastType             | Report corruption;  | Append into buffer; | -               | -                  |
// | RecyclableLastType   | End with            | End with            |                 |                    |
// |                      | no record read      | record returned     |                 |                    |
// +----------------------+---------------------+---------------------+-----------------+--------------------+
// | BadHeader            | Report corruption;  | Report corruption;  | -               | -                  |
// |                      | End with            | End with            |                 |                    |
// |                      | no record read      | no record read      |                 |                    |
// +----------------------+---------------------+---------------------+-----------------+--------------------+
// | MeetEOF              | Report corruption;  | Report corruption;  | -               | -                  |
// |                      | End with            | End with            |                 |                    |
// |                      | no record read      | no record read      |                 |                    |
// +----------------------+---------------------+---------------------+-----------------+--------------------+
// | OldRecord            | Report corruption;  | Report corruption;  | -               | -                  |
// |                      | End with            | End with            |                 |                    |
// |                      | no record read      | no record read      |                 |                    |
// +----------------------+---------------------+---------------------+-----------------+--------------------+
// | BadRecord            | Report corruption;  | Report corruption;  | -               | -                  |
// |                      | End with            | End with            |                 |                    |
// |                      | no record read      | no record read      |                 |                    |
// +----------------------+---------------------+---------------------+-----------------+--------------------+
// | BadRecordLen         | Report corruption;  | Report corruption;  | -               | -                  |
// |                      | End with            | End with            |                 |                    |
// |                      | no record read      | no record read      |                 |                    |
// +----------------------+---------------------+---------------------+-----------------+--------------------+
// | BadRecordChecksum    | Report corruption;  | Report corruption;  | -               | -                  |
// |                      | End with            | End with            |                 |                    |
// |                      | no record read      | no record read      |                 |                    |
// +----------------------+---------------------+---------------------+-----------------+--------------------+
std::tuple<bool, String> LogReader::readRecord()
{
    String record;
    bool in_fragmented_record = false;

    static_assert(
        std::is_same_v<std::underlying_type_t<Format::RecordType>, UInt8>,
        "The underlying type of Format::RecordType should be UInt8");
    static_assert(
        std::is_same_v<std::underlying_type_t<ParseErrorType>, UInt8>,
        "The underlying type of ParseErrorType should be UInt8");

    std::string_view fragment;
    while (true)
    {
        size_t drop_size = 0;
        const UInt8 record_type_or_error = readPhysicalRecord(&fragment, &drop_size);
        switch (record_type_or_error)
        {
        case Format::RecordType::FullType:
        case Format::RecordType::RecyclableFullType:
        {
            if (in_fragmented_record && !record.empty())
            {
                // Handle bug in earlier versions of log::Writer where
                // it could emit an empty Format::FirstType record at the tail end
                // of a block followed by a Format::FullType or Format::FirstType record
                // at the beginning of the next block.
                reportCorruption(record.size(), "partial record without end(1)");
            }
            record.assign(fragment.data(), fragment.size());
            return {true, std::move(record)};
        }

        case Format::RecordType::FirstType:
        case Format::RecordType::RecyclableFirstType:
        {
            if (in_fragmented_record && !record.empty())
            {
                // Handle bug in earlier versions of log::Writer where
                // it could emit an empty Format::FirstType record at the tail end
                // of a block followed by a Format::FullType or Format::FirstType record
                // at the beginning of the next block.
                reportCorruption(record.size(), "partial record without end(2)");
            }
            record.assign(fragment.data(), fragment.size());
            in_fragmented_record = true;
            break;
        }
        case Format::RecordType::MiddleType:
        case Format::RecordType::RecyclableMiddleType:
        {
            if (!in_fragmented_record)
            {
                reportCorruption(fragment.size(), "missing start of fragmented record(1)");
            }
            else
            {
                record.append(fragment.data(), fragment.size());
            }
            break;
        }
        case Format::RecordType::LastType:
        case Format::RecordType::RecyclableLastType:
        {
            if (!in_fragmented_record)
            {
                reportCorruption(fragment.size(), "missing start of fragmented record(2)");
            }
            else
            {
                record.append(fragment.data(), fragment.size());
                return {true, std::move(record)};
            }
            break;
        } // End of record_type in Format::RecordType

        // For enum defined in ParseErrorType
        default:
        {
            switch (record_type_or_error)
            {
            case ParseErrorType::BadHeader:
            {
                if (recovery_mode == WALRecoveryMode::AbsoluteConsistency
                    || recovery_mode == WALRecoveryMode::PointInTimeRecovery)
                {
                    // In clean shutdown we don't expect any error in the log files.
                    // In point-in-time recovery an incomplete record at the end could produce
                    // a hole in the recovered data. Report an error here, which higher layers
                    // can choose to ignore when it's provable there is no hole.
                    reportCorruption(drop_size, "truncated header");
                }
                [[fallthrough]];
            }
            case ParseErrorType::MeetEOF:
            {
                if (in_fragmented_record)
                {
                    if (recovery_mode == WALRecoveryMode::AbsoluteConsistency
                        || recovery_mode == WALRecoveryMode::PointInTimeRecovery)
                    {
                        // In clean shutdown we don't expect any error in the log files.
                        // In point-in-time recovery an incomplete record at the end could produce
                        // a hole in the recovered data. Report an error here, which higher layers
                        // can choose to ignore when it's provable there is no hole.
                        reportCorruption(record.size(), "error reading trailing data");
                    }
                    // This can be caused by the writer dying immediately after writing a
                    // physical record but before completing the next; don't treat it as
                    // a corruption, just ignore the entire logical record.
                    record.clear();
                }
                eof = true;
                return {false, std::move(record)};
            }
            case ParseErrorType::OldRecord:
            {
                if (recovery_mode != WALRecoveryMode::SkipAnyCorruptedRecords)
                {
                    // Treat a record from a previous instance of the log as EOF.
                    if (in_fragmented_record)
                    {
                        if (recovery_mode == WALRecoveryMode::AbsoluteConsistency
                            || recovery_mode == WALRecoveryMode::PointInTimeRecovery)
                        {
                            // In clean shutdown we don't expect any error in the log files.
                            // In point-in-time recovery an incomplete record at the end could produce
                            // a hole in the recovered data. Report an error here, which higher layers
                            // can choose to ignore when it's provable there is no hole.
                            reportCorruption(record.size(), "error reading trailing data");
                        }
                        // This can be caused by the writer dying immediately after writing a
                        // physical record but before completing the next; don't treat it as
                        // a corruption, just ignore the entire logical record.
                        record.clear();
                    }
                    return {false, std::move(record)};
                }
                [[fallthrough]];
            }
            case ParseErrorType::BadRecord:
            {
                if (in_fragmented_record)
                {
                    reportCorruption(record.size(), "error in middle of record");
                    in_fragmented_record = false;
                    record.clear();
                }
                break;
            }
            case ParseErrorType::BadRecordLen:
            {
                if (is_last_block)
                {
                    if (recovery_mode == WALRecoveryMode::AbsoluteConsistency
                        || recovery_mode == WALRecoveryMode::PointInTimeRecovery)
                    {
                        // In clean shutdown we don't expect any error in the log files.
                        // In point-in-time recovery an incomplete record at the end could produce
                        // a hole in the recovered data. Report an error here, which higher layers
                        // can choose to ignore when it's provable there is no hole.
                        reportCorruption(drop_size, "truncated record body");
                    }
                    return {false, std::move(record)};
                }
                [[fallthrough]];
            }
            case ParseErrorType::BadRecordChecksum:
            {
                if (recycled && recovery_mode == WALRecoveryMode::TolerateCorruptedTailRecords)
                {
                    record.clear();
                    return {false, std::move(record)};
                }
                if (record_type_or_error == ParseErrorType::BadRecordLen)
                {
                    reportCorruption(drop_size, "bad record length");
                }
                else
                {
                    reportCorruption(drop_size, "checksum mismatch");
                }
                if (in_fragmented_record)
                {
                    reportCorruption(record.size(), "error in middle of record");
                    in_fragmented_record = false;
                    record.clear();
                }
                break;
            }
            default:
            {
                reportCorruption(
                    (fragment.size() + (in_fragmented_record ? record.size() : 0)),
                    fmt::format("unknown record type {}", record_type_or_error));
                in_fragmented_record = false;
                record.clear();
                break;
            }
            }
        } // End of record_type in ParseErrorType
        } // End of record_type in Format::RecordType or ParseErrorType
    }
    return {false, std::move(record)};
}

struct LogReader::RecyclableHeader
{
    Format::ChecksumType checksum = 0;
    UInt16 length = 0;
    UInt8 type = 0;
    Format::LogNumberType log_num = 0;
};

std::tuple<UInt8, size_t> LogReader::deserializeHeader(LogReader::RecyclableHeader * hdr, size_t * drop_size)
{
    readIntBinary(hdr->checksum, *file);
    readIntBinary(hdr->length, *file);
    readChar(hdr->type, *file);
    size_t header_size = Format::HEADER_SIZE;
    if (hdr->type >= Format::RecyclableFullType && hdr->type <= Format::RecyclableLastType)
    {
        if (end_of_buffer_offset - buffer.size() == 0)
        {
            recycled = true;
        }
        header_size = Format::RECYCLABLE_HEADER_SIZE;
        // We need enough for the larger header
        if (buffer.size() < static_cast<size_t>(Format::RECYCLABLE_HEADER_SIZE))
        {
            // Go to the next block and continue parsing
            if (UInt8 err = readMore(drop_size); err != 0)
                return {err, 0};
            return {ParseErrorType::NextBlock, 0};
        }
        Format::LogNumberType log_num = 0;
        readIntBinary(log_num, *file);
        if (log_num != log_number)
        {
            return {ParseErrorType::OldRecord, 0};
        }
    }

    if (header_size + hdr->length > buffer.size())
    {
        assert(buffer.size() >= header_size);
        *drop_size = buffer.size();
        buffer.remove_prefix(buffer.size());
        // If the end of the read has been reached without seeing
        // `header_size + length` bytes of payload, report a corruption. The
        // higher layers can decide how to handle it based on the recovery mode,
        // whether this occurred at EOF, whether this is the final WAL, etc.
        return {ParseErrorType::BadRecordLen, 0};
    }

    if (hdr->type == Format::ZeroType && hdr->length == 0)
    {
        // Skip zero length record without reporting any drops since
        // such records are produced by the mmap based writing code in
        // env_posix.cc that preallocates file regions.
        // NOTE: this should never happen in DB written by new RocksDB versions,
        // since we turn off mmap writes to manifest and log files
        buffer.remove_prefix(buffer.size());
        return {ParseErrorType::BadRecord, 0};
    }

    return {0, header_size};
}

UInt8 LogReader::readPhysicalRecord(std::string_view * result, size_t * drop_size)
{
    while (true)
    {
        // We need at least the minimum header size
        if (buffer.size() < static_cast<size_t>(Format::HEADER_SIZE))
        {
            // the default value of r is meaningless because ReadMore will overwrite it if it
            // returns false; in case it returns true, the return value will not be used anyway.
            if (UInt8 err = readMore(drop_size); err != 0)
                return err;
            continue;
        }

        // Parse the header
        const char * header_pos = buffer.data();
        LogReader::RecyclableHeader hdr;
        size_t header_size;
        UInt8 err;
        std::tie(err, header_size) = deserializeHeader(&hdr, drop_size);
        if (err == ParseErrorType::NextBlock)
        {
            // No enough bytes for deser header, continue parsing from the next block
            continue;
        }
        else if (err != 0)
            return err;
        // else parse header success.

        if (verify_checksum)
        {
            Format::ChecksumClass digest;
            digest.update(
                header_pos + Format::CHECKSUM_START_OFFSET,
                hdr.length + header_size - Format::CHECKSUM_START_OFFSET);
            if (Format::ChecksumType actual_checksum = digest.checksum(); actual_checksum != hdr.checksum)
            {
                // Drop the rest of the buffer since "length" itself may have
                // been corrupted and if we trust it, we could find some
                // fragment of a real log record that just happens to look
                // like a valid log record.
                *drop_size = buffer.size();
                buffer.remove_prefix(buffer.size());
                return ParseErrorType::BadRecordChecksum;
            }
        }

        buffer.remove_prefix(header_size + hdr.length);

        *result = std::string_view(header_pos + header_size, hdr.length);
        file->ignore(hdr.length); // Move forward the payload size
        return hdr.type;
    }
}

UInt8 LogReader::readMore(size_t * drop_size)
{
    static_assert(ParseErrorType::MeetEOF != 0 && ParseErrorType::BadHeader != 0);

    if (likely(!is_last_block && !read_error))
    {
        // Last read was a full read, so this is a trailer to skip
        buffer.remove_prefix(buffer.size());
        try
        {
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_when_read_from_log);
            file->next();
            buffer = std::string_view{file->buffer().begin(), file->buffer().size()};
            end_of_buffer_offset += buffer.size();
            if (buffer.size() < static_cast<size_t>(Format::BLOCK_SIZE))
            {
                is_last_block = true;
                eof_offset = buffer.size();
            }
            // Return "0" for no error happen
            return 0;
        }
        catch (...)
        {
            // Exception thrown while reading data by `file->next()`, consider it as meeting EOF
            reportDrop(
                Format::BLOCK_SIZE,
                fmt::format("while reading Log file: {}, {}", file->getFileName(), getCurrentExceptionMessage(true)));
            buffer.remove_prefix(buffer.size());
            read_error = true;
            return ParseErrorType::MeetEOF;
        }
    }

    // Note that if buffer is non-empty, we have a truncated header at the
    // end of the file, which can be caused by the writer crashing in the
    // middle of writing the header.
    if (!buffer.empty())
    {
        *drop_size = buffer.size();
        buffer.remove_prefix(buffer.size());
        return ParseErrorType::BadHeader;
    }

    return ParseErrorType::MeetEOF;
}

void LogReader::reportCorruption(size_t bytes, const String & reason)
{
    reportDrop(
        bytes,
        fmt::format("Corruption: {} [offset={}] [file={}]", reason, file->getPositionInFile(), file->getFileName()));
}

void LogReader::reportDrop(size_t bytes, const String & reason)
{
    reporter->corruption(bytes, reason);
}

} // namespace DB::PS::V3
