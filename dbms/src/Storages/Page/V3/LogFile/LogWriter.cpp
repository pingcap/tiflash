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
#include <Common/Logger.h>
#include <IO/BaseFile/WritableFile.h>
#include <IO/Buffer/ReadBuffer.h>
#include <IO/Buffer/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/PageUtil.h>
#include <Storages/Page/V3/LogFile/LogFormat.h>
#include <Storages/Page/V3/LogFile/LogWriter.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

namespace DB::PS::V3
{
LogWriter::LogWriter(
    String path_,
    const FileProviderPtr & file_provider_,
    Format::LogNumberType log_number_,
    bool recycle_log_files_,
    bool manual_sync_)
    : path(path_)
    , file_provider(file_provider_)
    , block_offset(0)
    , log_number(log_number_)
    , recycle_log_files(recycle_log_files_)
    , manual_sync(manual_sync_)
    , write_buffer(nullptr, 0)
{
    log_file = file_provider->newWritableFile(
        path,
        EncryptionPath(path, ""),
        false,
        /*create_new_encryption_info_*/ true);

    buffer = static_cast<char *>(alloc(buffer_size));
    RUNTIME_CHECK_MSG(buffer != nullptr, "LogWriter cannot allocate buffer, size={}", buffer_size);
    write_buffer = WriteBuffer(buffer, buffer_size);
}

void LogWriter::resetBuffer()
{
    write_buffer = WriteBuffer(buffer, buffer_size);
}

LogWriter::~LogWriter()
{
    log_file->fsync();
    log_file->close();

    free(buffer, buffer_size);
}

size_t LogWriter::writtenBytes() const
{
    return written_bytes;
}

void LogWriter::sync()
{
    log_file->fsync();
}

void LogWriter::close()
{
    log_file->close();
}

void LogWriter::addRecord(
    ReadBuffer & payload,
    const size_t payload_size,
    const WriteLimiterPtr & write_limiter,
    bool background)
{
    // Header size varies depending on whether we are recycling or not.
    const UInt32 header_size = recycle_log_files ? Format::RECYCLABLE_HEADER_SIZE : Format::HEADER_SIZE;

    // Fragment the record if necessary and emit it. Note that if payload is empty,
    // we still want to iterate once to emit a single zero-length record.
    bool begin = true;
    size_t payload_left = payload_size;
    // Padding current block if needed
    block_offset = block_offset % Format::BLOCK_SIZE; // 0 <= block_offset < Format::BLOCK_SIZE
    size_t leftover = Format::BLOCK_SIZE - block_offset;
    assert(leftover > 0);
    if (leftover < header_size)
    {
        // Fill the trailer with all zero
        static constexpr char MAX_ZERO_HEADER[Format::RECYCLABLE_HEADER_SIZE]{'\x00'};
        if (unlikely(buffer_size - write_buffer.offset() < leftover))
        {
            flush(write_limiter, background);
        }
        writeString(MAX_ZERO_HEADER, leftover, write_buffer);
        block_offset = 0;
    }
    do
    {
        block_offset = block_offset % Format::BLOCK_SIZE;
        // Invariant: we never leave < header_size bytes in a block.
        assert(Format::BLOCK_SIZE - block_offset >= header_size);

        const size_t avail_payload_size = Format::BLOCK_SIZE - block_offset - header_size;
        const size_t fragment_length = (payload_left < avail_payload_size) ? payload_left : avail_payload_size;
        const bool end = (payload_left == fragment_length);
        Format::RecordType type;
        if (begin && end)
            type = recycle_log_files ? Format::RecordType::RecyclableFullType : Format::RecordType::FullType;
        else if (begin)
            type = recycle_log_files ? Format::RecordType::RecyclableFirstType : Format::RecordType::FirstType;
        else if (end)
            type = recycle_log_files ? Format::RecordType::RecyclableLastType : Format::RecordType::LastType;
        else
            type = recycle_log_files ? Format::RecordType::RecyclableMiddleType : Format::RecordType::MiddleType;
        // Check available space in write_buffer before writing
        if (buffer_size - write_buffer.offset() < fragment_length + header_size)
        {
            flush(write_limiter, background);
        }
        try
        {
            emitPhysicalRecord(type, payload, fragment_length);
        }
        catch (...)
        {
            auto message = getCurrentExceptionMessage(true);
            LOG_FATAL(Logger::get(), "Write physical record failed with message: {}", message);
            std::terminate();
        }
        payload.ignore(fragment_length);
        payload_left -= fragment_length;
        begin = false;
    } while (payload.hasPendingData());

    flush(write_limiter, background);
    if (!manual_sync)
    {
        sync();
    }
}

void LogWriter::emitPhysicalRecord(Format::RecordType type, ReadBuffer & payload, size_t length)
{
    assert(length <= 0xFFFF); // The length of payload must fit in two bytes (less than `BLOCK_SIZE`)

    // Create a header buffer without the checksum field
    static_assert(
        Format::RECYCLABLE_HEADER_SIZE > Format::CHECKSUM_FIELD_SIZE,
        "Header size must be greater than the checksum size");
    static_assert(
        Format::RECYCLABLE_HEADER_SIZE > Format::HEADER_SIZE,
        "Ensure the min buffer size for physical record");
    constexpr static size_t HEADER_BUFF_SIZE = Format::RECYCLABLE_HEADER_SIZE - Format::CHECKSUM_FIELD_SIZE;
    char buf[HEADER_BUFF_SIZE] = {0};
    WriteBuffer header_buff(buf, HEADER_BUFF_SIZE);

    // Format the header
    writeIntBinary(static_cast<UInt16>(length), header_buff);
    writeChar(static_cast<char>(type), header_buff);

    // The checksum range:
    // Header: [type, payload-size]
    // RecyclabelHeader: [type, log_number, payload-size]
    Format::ChecksumClass digest;
    char ch_type = static_cast<char>(type);
    digest.update(&ch_type, sizeof(char));
    size_t header_size;
    if (type < Format::RecyclableFullType)
    {
        // Legacy record format
        assert(block_offset + Format::HEADER_SIZE + length <= Format::BLOCK_SIZE);
        header_size = Format::HEADER_SIZE;
    }
    else
    {
        // Recyclabel record format
        assert(block_offset + Format::RECYCLABLE_HEADER_SIZE + length <= Format::BLOCK_SIZE);
        header_size = Format::RECYCLABLE_HEADER_SIZE;

        // We will fail to detect an old record if we recycled a log from
        // ~4 billion logs ago, but that is effectively impossible, and
        // even if it were we'dbe far more likely to see a false positive
        // on the checksum.
        writeIntBinary(log_number, header_buff);
        digest.update(header_buff.position() - sizeof(Format::LogNumberType), sizeof(Format::LogNumberType));
    }

    // Compute the checksum of the record type and the payload.
    // Write the checksum, header and the payload
    digest.update(payload.position(), length);
    Format::ChecksumType checksum = digest.checksum();
    writeIntBinary(checksum, write_buffer);
    writeString(header_buff.buffer().begin(), header_buff.count(), write_buffer);
    writeString(payload.position(), length, write_buffer);

    block_offset += header_size + length;
}

void LogWriter::flush(const WriteLimiterPtr & write_limiter, bool background)
{
    if (write_buffer.offset() == 0)
    {
        return;
    }

    PageUtil::writeFile(
        log_file,
        written_bytes,
        write_buffer.buffer().begin(),
        write_buffer.offset(),
        write_limiter,
        /*background=*/background,
        /*truncate_if_failed=*/false,
        /*enable_failpoint=*/false);

    written_bytes += write_buffer.offset();

    // reset the write_buffer
    resetBuffer();
}
} // namespace DB::PS::V3
