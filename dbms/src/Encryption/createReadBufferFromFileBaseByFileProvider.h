#pragma once

#include <Encryption/FileProvider.h>
#include <IO/ReadBufferFromFileBase.h>
#include <memory>
#include <string>


namespace DB
{

/** Create an object to read data from a file.
  * estimated_size - the number of bytes to read
  * aio_threshold - the minimum number of bytes for asynchronous reads
  *
  * Caution: (AIO is not supported yet.)
  *
  * If aio_threshold = 0 or estimated_size < aio_threshold, read operations are executed synchronously.
  * Otherwise, the read operations are performed asynchronously.
  */

std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBaseByFileProvider(FileProviderPtr & file_provider,
    const std::string & filename_,
    const EncryptionPath & encryption_path_,
    size_t estimated_size,
    size_t aio_threshold,
    size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
    int flags_ = -1,
    char * existing_memory_ = nullptr,
    size_t alignment = 0);
} // namespace DB
