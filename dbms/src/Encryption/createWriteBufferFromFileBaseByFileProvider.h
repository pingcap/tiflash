#pragma once

#include <Common/Checksum.h>
#include <Encryption/FileProvider.h>
#include <IO/WriteBufferFromFileBase.h>

#include <string>

namespace DB
{
struct WriteBufferParameterTable
{
    bool has_checksum;
    const FileProviderPtr & file_provider;
    const std::string & filename;
    const EncryptionPath & encryption_path;
    bool create_new_encryption_info;
    const WriteLimiterPtr & write_limiter;
    int flags = -1;
    mode_t mode = 0666;

    // legacy
    size_t estimated_size = 0;
    size_t aio_threshold = 0;
    size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    size_t alignment = 0;
    char * existing_memory = nullptr;

    // checksum
    ChecksumAlgo checksum_algorithm = ChecksumAlgo::None;
    size_t checksum_frame_size = DBMS_DEFAULT_BUFFER_SIZE;
};
/** Create an object to write data to a file.
  * estimated_size - number of bytes to write
  * aio_threshold - the minimum number of bytes for asynchronous writes
  *
  * Caution: (AIO is not supported yet.)
  *
  * If aio_threshold = 0 or estimated_size < aio_threshold, the write operations are executed synchronously.
  * Otherwise, write operations are performed asynchronously.
  */
std::unique_ptr<WriteBufferFromFileBase>
createWriteBufferFromFileBaseByFileProvider(
    const FileProviderPtr & file_provider,
    const std::string & filename_,
    const EncryptionPath & encryption_path_,
    bool create_new_encryption_info_,
    const WriteLimiterPtr & write_limiter_,
    size_t estimated_size,
    size_t aio_threshold,
    size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
    int flags_ = -1,
    mode_t mode = 0666,
    char * existing_memory_ = nullptr,
    size_t alignment = 0);

std::unique_ptr<WriteBufferFromFileBase>
createWriteBufferFromFileBaseByFileProvider(
    const FileProviderPtr & file_provider,
    const std::string & filename_,
    const EncryptionPath & encryption_path_,
    bool create_new_encryption_info_,
    const WriteLimiterPtr & write_limiter_,
    ChecksumAlgo checksum_algorithm,
    size_t checksum_frame_size,
    int flags_ = -1,
    mode_t mode = 0666);

static inline std::unique_ptr<WriteBufferFromFileBase>
createWriteBufferFromFileBaseByFileProvider(
    WriteBufferParameterTable parameter_table)
{
    if (parameter_table.has_checksum)
    {
        return createWriteBufferFromFileBaseByFileProvider(
            parameter_table.file_provider,
            parameter_table.filename,
            parameter_table.encryption_path,
            parameter_table.create_new_encryption_info,
            parameter_table.write_limiter,
            parameter_table.checksum_algorithm,
            parameter_table.checksum_frame_size,
            parameter_table.flags,
            parameter_table.mode);
    }
    else
    {
        return createWriteBufferFromFileBaseByFileProvider(
            parameter_table.file_provider,
            parameter_table.filename,
            parameter_table.encryption_path,
            parameter_table.create_new_encryption_info,
            parameter_table.write_limiter,
            parameter_table.estimated_size,
            parameter_table.aio_threshold,
            parameter_table.buffer_size,
            parameter_table.flags,
            parameter_table.mode,
            parameter_table.existing_memory,
            parameter_table.alignment);
    }
};
} // namespace DB
