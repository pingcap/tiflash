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

#pragma once

#include <Common/nocopyable.h>
#include <IO/Buffer/ReadBufferFromFileBase.h>
#include <Storages/Page/V3/LogFile/LogFormat.h>
#include <Storages/Page/V3/WALStore.h>
#include <common/types.h>

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
        WALRecoveryMode recovery_mode_);

    DISALLOW_COPY(LogReader);

    virtual ~LogReader();

    // Read the next record record.  Returns <true, record> if read
    // successfully, false if we hit end of the input.
    virtual std::tuple<bool, String> readRecord();

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
    bool is_last_block; // Last Read() indicated EOF by returning < BlockSize
    bool eof;
    bool read_error; // Error occrured while reading from file
    // Offset of the file position indicator within the last block when an EOF was detected.
    WALRecoveryMode recovery_mode;
    size_t eof_offset;

    const std::unique_ptr<ReadBufferFromFileBase> file;
    std::string_view buffer;
    Reporter * const reporter;

    UInt64 end_of_buffer_offset;
    // which log number it is
    const Format::LogNumberType log_number;
};

} // namespace PS::V3
} // namespace DB
