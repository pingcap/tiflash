#pragma once

#include <Common/Checksum.h>
#include <Encryption/FileProvider.h>
#include <IO/WriteBufferFromFileBase.h>

#include <string>

namespace DB
{
/** Create an object to write data to a file.
  * estimated_size - number of bytes to write
  * aio_threshold - the minimum number of bytes for asynchronous writes
  *
  * Caution: (AIO is not supported yet.)
  *
  * If aio_threshold = 0 or estimated_size < aio_threshold, the write operations are executed synchronously.
  * Otherwise, write operations are performed asynchronously.
  */
WriteBufferFromFileBase *
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
} // namespace DB
