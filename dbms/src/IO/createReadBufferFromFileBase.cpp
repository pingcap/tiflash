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

#include <IO/ReadBufferFromFile.h>
#include <IO/createReadBufferFromFileBase.h>

#if !defined(__APPLE__) && !defined(__FreeBSD__) && !defined(_MSC_VER)
#include <IO/ReadBufferAIO.h>
#endif
#include <Common/ProfileEvents.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

std::unique_ptr<ReadBufferFromFileBase> createReadBufferFromFileBase(
    const std::string & filename_,
    size_t estimated_size,
    size_t aio_threshold,
    size_t buffer_size_,
    int flags_,
    char * existing_memory_,
    size_t alignment)
{
    if ((aio_threshold == 0) || (estimated_size < aio_threshold))
    {
        return std::make_unique<ReadBufferFromFile>(filename_, buffer_size_, flags_, existing_memory_, alignment);
    }
    else
    {
#if !defined(__APPLE__) && !defined(__FreeBSD__) && !defined(_MSC_VER)
        return std::make_unique<ReadBufferAIO>(filename_, buffer_size_, flags_, existing_memory_);
#else
        throw Exception("AIO is not implemented yet on MacOS X", ErrorCodes::NOT_IMPLEMENTED);
#endif
    }
}

} // namespace DB
