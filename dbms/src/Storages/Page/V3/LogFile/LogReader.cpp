#include <Common/Checksum.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/RedactHelpers.h>
#include <IO/ReadBufferFromMemory.h>
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
    uint64_t log_num_,
    Poco::Logger * log_)
    : verify_checksum(verify_checksum_)
    , recycled(false)
    , eof(false)
    , read_error(false)
    , eof_offset(0)
    , file(std::move(file_))
    , buffer(file->buffer().begin(), file->buffer().size())
    , reporter(reporter_)
    , last_record_offset(0)
    , end_of_buffer_offset(0)
    , log_number(log_num_)
    , log(log_)
{
}

LogReader::~LogReader() = default;

std::tuple<bool, String> LogReader::readRecord(WALRecoveryMode wal_recovery_mode)
{
    String record;
    bool in_fragmented_record = false;
    // Record offset of the logical record that we're reading
    // 0 is a dummy value to make compilers happy
    uint64_t prospective_record_offset = 0;

    std::string_view fragment;
    while (true)
    {
        uint64_t physical_record_offset = end_of_buffer_offset - buffer.size();
        size_t drop_size = 0;
        static_assert(
            std::is_same_v<std::underlying_type_t<ParseErrorType>, uint8_t>,
            "The underlying type of ParseErrorType should be uint8_t");
        const uint8_t record_type = readPhysicalRecord(&fragment, &drop_size);
        switch (record_type)
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
            prospective_record_offset = physical_record_offset;
            record.assign(fragment.data(), fragment.size());
            last_record_offset = prospective_record_offset;
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
            prospective_record_offset = physical_record_offset;
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
                last_record_offset = prospective_record_offset;
                return {true, std::move(record)};
            }
            break;
        } // End of record_type in Format::RecordType

        // For enum defined in ParseErrorType
        default:
        {
            switch (record_type)
            {
            case ParseErrorType::BadHeader:
            {
                if (wal_recovery_mode == WALRecoveryMode::AbsoluteConsistency || wal_recovery_mode == WALRecoveryMode::PointInTimeRecovery)
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
                    if (wal_recovery_mode == WALRecoveryMode::AbsoluteConsistency || wal_recovery_mode == WALRecoveryMode::PointInTimeRecovery)
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
            case ParseErrorType::OldRecord:
            {
                if (wal_recovery_mode != WALRecoveryMode::SkipAnyCorruptedRecords)
                {
                    // Treat a record from a previous instance of the log as EOF.
                    if (in_fragmented_record)
                    {
                        if (wal_recovery_mode == WALRecoveryMode::AbsoluteConsistency || wal_recovery_mode == WALRecoveryMode::PointInTimeRecovery)
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
                if (eof)
                {
                    if (wal_recovery_mode == WALRecoveryMode::AbsoluteConsistency || wal_recovery_mode == WALRecoveryMode::PointInTimeRecovery)
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
                if (recycled && wal_recovery_mode == WALRecoveryMode::TolerateCorruptedTailRecords)
                {
                    record.clear();
                    return {false, std::move(record)};
                }
                if (record_type == ParseErrorType::BadRecordLen)
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
                reportCorruption((fragment.size() + (in_fragmented_record ? record.size() : 0)), fmt::format("unknown record type {}", record_type));
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

uint8_t LogReader::readPhysicalRecord(std::string_view * result, size_t * drop_size)
{
    while (true)
    {
        // We need at least the minimum header size
        if (buffer.size() < static_cast<size_t>(Format::HEADER_SIZE))
        {
            // the default value of r is meaningless because ReadMore will overwrite it if it
            // returns false; in case it returns true, the return value will not be used anyway.
            if (uint8_t r = ParseErrorType::MeetEOF; !readMore(drop_size, &r))
                return r;
            continue;
        }

        // Parse the header
        const char * header = buffer.data();
        uint32_t expected_checksum;
        uint16_t length = 0;
        char type;
        readIntBinary(expected_checksum, *file);
        readIntBinary(length, *file);
        readChar(type, *file);
        int header_size = Format::HEADER_SIZE;
        if (type >= Format::RecyclableFullType && type <= Format::RecyclableLastType)
        {
            if (end_of_buffer_offset - buffer.size() == 0)
            {
                recycled = true;
            }
            header_size = Format::RECYCLABLE_HEADER_SIZE;
            // We need enough for the larger header
            if (buffer.size() < static_cast<size_t>(Format::RECYCLABLE_HEADER_SIZE))
            {
                if (uint8_t r = ParseErrorType::MeetEOF; !readMore(drop_size, &r))
                    return r;
                continue;
            }
            uint32_t log_num = 0;
            readIntBinary(log_num, *file);
            if (log_num != log_number)
            {
                return ParseErrorType::OldRecord;
            }
        }
        if (header_size + UInt32(length) > buffer.size())
        {
            assert(buffer.size() >= static_cast<size_t>(header_size));
            *drop_size = buffer.size();
            buffer.remove_prefix(buffer.size());
            // If the end of the read has been reached without seeing
            // `header_size + length` bytes of payload, report a corruption. The
            // higher layers can decide how to handle it based on the recovery mode,
            // whether this occurred at EOF, whether this is the final WAL, etc.
            return ParseErrorType::BadRecordLen;
        }

        if (type == Format::ZeroType && length == 0)
        {
            // Skip zero length record without reporting any drops since
            // such records are produced by the mmap based writing code in
            // env_posix.cc that preallocates file regions.
            // NOTE: this should never happen in DB written by new RocksDB versions,
            // since we turn off mmap writes to manifest and log files
            buffer.remove_prefix(buffer.size());
            return ParseErrorType::BadRecord;
        }

        if (verify_checksum)
        {
            Format::ChecksumClass digest;
            digest.update(header + 6, length + header_size - 6);
            if (uint32_t actual_checksum = digest.checksum(); actual_checksum != expected_checksum)
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

        buffer.remove_prefix(header_size + length);

        *result = std::string_view(header + header_size, length);
        file->ignore(length); // Move forward the payload size
        return type;
    }
}

bool LogReader::readMore(size_t * drop_size, uint8_t * error)
{
    if (likely(!eof && !read_error))
    {
        // Last read was a full read, so this is a trailer to skip
        buffer.remove_prefix(buffer.size());
        try
        {
            // Last read was a full read, so this is a trailer to skip
            FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_when_read_from_log);
            file->next();
            buffer = std::string_view{file->buffer().begin(), file->buffer().size()};
            end_of_buffer_offset += buffer.size();
            if (buffer.size() < static_cast<size_t>(Format::BLOCK_SIZE))
            {
                eof = true;
                eof_offset = buffer.size();
            }
            return true;
        }
        catch (...)
        {
            reportDrop(Format::BLOCK_SIZE, fmt::format("while reading Log file: {}, {}", file->getFileName(), getCurrentExceptionMessage(true)));
            buffer.remove_prefix(buffer.size());
            read_error = true;
            *error = ParseErrorType::MeetEOF;
            return false;
        }
    }

    // Note that if buffer is non-empty, we have a truncated header at the
    // end of the file, which can be caused by the writer crashing in the
    // middle of writing the header. Unless explicitly requested we don't
    // considering this an error, just report EOF.
    if (!buffer.empty())
    {
        *drop_size = buffer.size();
        buffer.remove_prefix(buffer.size());
        *error = ParseErrorType::BadHeader;
        return false;
    }

    buffer.remove_prefix(buffer.size());
    *error = ParseErrorType::MeetEOF;
    return false;
}

void LogReader::reportCorruption(size_t bytes, const String & reason)
{
    reportDrop(bytes, "Corruption: " + reason);
}

void LogReader::reportDrop(size_t bytes, const String & reason)
{
    reporter->corruption(bytes, reason);
}

} // namespace DB::PS::V3
