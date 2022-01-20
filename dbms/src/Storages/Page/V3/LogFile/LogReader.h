#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <Storages/Page/V3/LogFile/LogFormat.h>
#include <Storages/Page/V3/WALStore.h>
#include <common/types.h>

namespace Poco
{
class Logger;
}
namespace DB
{
class ReadBuffer;
class WriteBufferFromFile;
namespace PS::V3
{
class LogReader
{
public:
    // Interface for reporting errors.
    class Reporter
    {
    public:
        virtual ~Reporter() = default;

        // Some corruption was detected.  "size" is the approximate number
        // of bytes dropped due to the corruption.
        virtual void corruption(size_t bytes, const String & reason) = 0;
    };

public:
    LogReader(
        std::unique_ptr<ReadBufferFromFileBase> && file_,
        Reporter * reporter_,
        bool verify_checksum_,
        Format::LogNumberType log_num_,
        WALRecoveryMode recovery_mode_,
        Poco::Logger * log_);

    LogReader(const LogReader &) = delete;
    LogReader & operator=(const LogReader &) = delete;

    virtual ~LogReader();

    // Read the next record record.  Returns <true, record> if read
    // successfully, false if we hit end of the input.
    virtual std::tuple<bool, String> readRecord();

    // Returns the physical offset of the last record returned by readRecord.
    //
    // Undefined before the first call to readRecord.
    UInt64 lastRecordOffset();

    bool isEOF() const { return eof; }

    Format::LogNumberType getLogNumber() const { return log_number; }

protected:
    // Reports dropped bytes to the reporter.
    // `buffer` must be updated to remove the dropped bytes prior to invocation.
    void reportCorruption(size_t bytes, const String & reason);
    void reportDrop(size_t bytes, const String & reason);

private:
    // Extend record types with the following special values
    enum ParseErrorType : UInt8
    {
        MeetEOF = Format::MaxRecordType + 1,
        // Returned whenever we find an invalid physical record.
        // Currently there are three situations in which this happens:
        // * The record has an invalid checksum (ReadPhysicalRecord reports a drop)
        // * The record is a 0-length record (No drop is reported)
        BadRecord = Format::MaxRecordType + 2,
        // Returned when we fail to read a valid header.
        BadHeader = Format::MaxRecordType + 3,
        // Returned when we read an old record from a previous user of the log.
        OldRecord = Format::MaxRecordType + 4,
        // Returned when we get a bad record length
        BadRecordLen = Format::MaxRecordType + 5,
        // Returned when we get a bad record checksum
        BadRecordChecksum = Format::MaxRecordType + 6,
        NextBlock = Format::MaxRecordType + 7,
    };

    UInt8 readPhysicalRecord(std::string_view * result, size_t * drop_size);

    struct RecyclableHeader;
    /*
     * Deserialize header from `file` and apply some check by the header field
     * Return 0 if deser success.
     * Else if no enough bytes for deser, return `ParseErrorType::NextBlock`
     * Otherwise return non-zero error.
     */
    std::tuple<UInt8, size_t> deserializeHeader(RecyclableHeader * hdr, size_t * drop_size);

    /*
     * Read more data from `file` and update the `buffer`.
     * Return 0 if read success.
     * Otherwise return `ParseErrorType::MeetEOF` or `ParseErrorType::BadHeader`
     */
    UInt8 readMore(size_t * drop_size);

private:
    const bool verify_checksum;
    bool recycled;
    bool eof; // Last Read() indicated EOF by returning < BlockSize
    bool read_error; // Error occrured while reading from file
    // Offset of the file position indicator within the last block when an EOF was detected.
    WALRecoveryMode recovery_mode;
    size_t eof_offset;

    const std::unique_ptr<ReadBufferFromFileBase> file;
    std::string_view buffer;
    Reporter * reporter;

    UInt64 last_record_offset;
    UInt64 end_of_buffer_offset;
    // which log number it is
    Format::LogNumberType log_number;

    Poco::Logger * log;
};

} // namespace PS::V3
} // namespace DB
