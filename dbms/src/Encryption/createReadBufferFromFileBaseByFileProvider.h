// Copyright 2022 PingCAP, Ltd.
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

#include <Common/Checksum.h>
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

std::unique_ptr<ReadBufferFromFileBase>
createReadBufferFromFileBaseByFileProvider(
    FileProviderPtr & file_provider,
    const std::string & filename_,
    const EncryptionPath & encryption_path_,
    size_t estimated_size,
    size_t aio_threshold,
    const ReadLimiterPtr & read_limiter,
    size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
    int flags_ = -1,
    char * existing_memory_ = nullptr,
    size_t alignment = 0);

/// @attention: estimated_size should be at least DBMS_DEFAULT_BUFFER_SIZE if one want to do seeking; however, if one knows that target file
/// only consists of a single small frame, one can use a smaller estimated_size to reduce memory footprint.
std::unique_ptr<ReadBufferFromFileBase>
createReadBufferFromFileBaseByFileProvider(
    const FileProviderPtr & file_provider,
    const std::string & filename_,
    const EncryptionPath & encryption_path_,
    size_t estimated_size,
    const ReadLimiterPtr & read_limiter,
    ChecksumAlgo checksum_algorithm,
    size_t checksum_frame_size,
    int flags_ = -1);
} // namespace DB
