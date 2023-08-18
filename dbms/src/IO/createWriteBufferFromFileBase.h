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

#include <IO/WriteBufferFromFileBase.h>

#include <string>

namespace DB
{
/** Create an object to write data to a file.
  * estimated_size - number of bytes to write
  * aio_threshold - the minimum number of bytes for asynchronous writes
  *
  * If aio_threshold = 0 or estimated_size < aio_threshold, the write operations are executed synchronously.
  * Otherwise, write operations are performed asynchronously.
  */
WriteBufferFromFileBase * createWriteBufferFromFileBase(
    const std::string & filename_,
    size_t estimated_size,
    size_t aio_threshold,
    size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE,
    int flags_ = -1,
    mode_t mode = 0666,
    char * existing_memory_ = nullptr,
    size_t alignment = 0);
} // namespace DB
