#pragma once

#include <Encryption/FileProvider.h>
#include <Storages/Page/V3/LogFile/LogFormat.h>
#include <common/types.h>

namespace DB
{
class ReadBuffer;
class WriteBufferFromFileBase;
namespace PS::V3
{
/**
 * Writer is a general purpose log stream writer. It provides an append-only
 * abstraction for writing data. The details of the how the data is written is
 * handled by the WriteableFile sub-class implementation.
 *
 * Use the same format as rocksdb write ahead log file
 * (https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format)
 *
 * File format:
 *
 * File is broken down into variable sized records. The format of each record
 * is described below.
 *       +-----+-------------+--+----+----------+------+-- ... ----+
 * File  | r0  |        r1   |P | r2 |    r3    |  r4  |           |
 *       +-----+-------------+--+----+----------+------+-- ... ----+
 *       <--- kBlockSize ------>|<-- kBlockSize ------>|
 *  rn = variable size records
 *  P = Padding
 *
 * Data is written out in kBlockSize chunks. If next record does not fit
 * into the space left, the leftover space will be padded with \0.
 *
 * Legacy record format:
 *
 * +--------------+-----------+-----------+--- ... ---+
 * |CheckSum (4B) | Size (2B) | Type (1B) | Payload   |
 * +--------------+-----------+-----------+--- ... ---+
 *
 * CheckSum = 32bit hash computed over the record type and payload using checksum algo
 * Size = Length of the payload data
 * Type = Type of record
 *        (kZeroType, kFullType, kFirstType, kLastType, kMiddleType )
 *        The type is used to group a bunch of records together to represent
 *        blocks that are larger than kBlockSize
 * Payload = Byte stream as long as specified by the payload size
 *
 * Recyclable record format:
 *
 * +--------------+-----------+-----------+----------------+--- ... ---+
 * |CheckSum (4B) | Size (2B) | Type (1B) | Log number (4B)| Payload   |
 * +--------------+-----------+-----------+----------------+--- ... ---+
 *
 * Same as above, with the addition of
 * Log number = 32bit log file number, so that we can distinguish between
 * records written by the most recent log writer vs a previous one.
 */
class LogWriter final : private Allocator<false>
{
public:
    LogWriter(
        String path_,
        const FileProviderPtr & file_provider_,
        Format::LogNumberType log_number_,
        bool recycle_log_files_,
        bool manual_flush_ = false);

    LogWriter(const LogWriter &) = delete;
    LogWriter & operator=(const LogWriter &) = delete;

    ~LogWriter();

    void addRecord(ReadBuffer & payload, size_t payload_size, const WriteLimiterPtr & write_limiter = nullptr);

    void flush(const WriteLimiterPtr & write_limiter = nullptr);

    void close();

    size_t writtenBytes() const;

    Format::LogNumberType logNumber() const
    {
        return log_number;
    }

private:
    void emitPhysicalRecord(Format::RecordType type, ReadBuffer & payload, size_t length);

    void resetBuffer();

private:
    String path;
    FileProviderPtr file_provider;

    WritableFilePtr log_file;

    size_t block_offset; // Current offset in block
    Format::LogNumberType log_number;
    const bool recycle_log_files;
    // If true, it does not flush after each write. Instead it relies on the upper
    // layer to manually does the flush by calling ::flush()
    const bool manual_flush;

    size_t written_bytes = 0;

    char * buffer;
    size_t buffer_size = Format::BLOCK_SIZE;
    WriteBuffer write_buffer;
};
} // namespace PS::V3
} // namespace DB
