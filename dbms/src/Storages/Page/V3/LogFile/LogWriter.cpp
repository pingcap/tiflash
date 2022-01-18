#include <Common/Checksum.h>
#include <Common/Exception.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Poco/Logger.h>
#include <Storages/Page/V3/LogFile/LogFormat.h>
#include <Storages/Page/V3/LogFile/LogWriter.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

namespace DB::PS::V3
{
LogWriter::LogWriter(
    std::unique_ptr<WriteBufferFromFileBase> && dest_,
    Format::LogNumberType log_number_,
    bool recycle_log_files_,
    bool manual_flush_)
    : dest(std::move(dest_))
    , block_offset(0)
    , log_number(log_number_)
    , recycle_log_files(recycle_log_files_)
    , manual_flush(manual_flush_)
{
    // Must be `BLOCK_SIZE`, or we can not ensure the correctness of writing.
    assert(dest->internalBuffer().size() == Format::BLOCK_SIZE);
}

LogWriter::~LogWriter()
{
    if (dest)
    {
        flush();
    }
}

size_t LogWriter::writtenBytes() const
{
    return dest->getMaterializedBytes();
}

void LogWriter::flush()
{
    dest->sync();
}

void LogWriter::close()
{
    if (dest)
    {
        dest->close();
        dest.reset();
    }
}

void LogWriter::addRecord(ReadBuffer & payload, const size_t payload_size)
{
    // Header size varies depending on whether we are recycling or not.
    const int header_size = recycle_log_files ? Format::RECYCLABLE_HEADER_SIZE : Format::HEADER_SIZE;

    // Fragment the record if necessary and emit it. Note that if payload is empty,
    // we still want to iterate once to emit a single zero-length record.
    bool begin = true;
    size_t payload_left = payload_size;
    do
    {
        const Int64 leftover = Format::BLOCK_SIZE - block_offset;
        assert(leftover >= 0);
        if (leftover < header_size)
        {
            // Switch to a new block
            if (leftover > 0)
            {
                // Fill the trailer with all zero
                static constexpr char MAX_ZERO_HEADER[Format::RECYCLABLE_HEADER_SIZE]{'\x00'};
                writeString(MAX_ZERO_HEADER, leftover, *dest);
            }
            block_offset = 0;
        }
        // Invariant: we never leave < header_size bytes in a block.
        assert(static_cast<Int64>(Format::BLOCK_SIZE - block_offset) >= header_size);

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
        emitPhysicalRecord(type, payload, fragment_length);
        payload.ignore(fragment_length);
        payload_left -= fragment_length;
        begin = false;
    } while (payload.hasPendingData());

    if (!manual_flush)
        dest->sync();
}

void LogWriter::emitPhysicalRecord(Format::RecordType type, ReadBuffer & payload, size_t length)
{
    assert(length <= 0xFFFF); // The length of payload must fit in two bytes (less than `BLOCK_SIZE`)

    // Create a header buffer without the checksum field
    static_assert(Format::RECYCLABLE_HEADER_SIZE > Format::CHECKSUM_FIELD_SIZE, "Header size must be greater than the checksum size");
    static_assert(Format::RECYCLABLE_HEADER_SIZE > Format::HEADER_SIZE, "Ensure the min buffer size for physical record");
    constexpr static size_t HEADER_BUFF_SIZE = Format::RECYCLABLE_HEADER_SIZE - Format::CHECKSUM_FIELD_SIZE;
    char buf[HEADER_BUFF_SIZE];
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
    writeIntBinary(checksum, *dest);
    writeString(header_buff.buffer().begin(), header_buff.count(), *dest);
    writeString(payload.position(), length, *dest);

    block_offset += header_size + length;
}
} // namespace DB::PS::V3
