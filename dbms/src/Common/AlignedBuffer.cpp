// Copyright 2025 PingCAP, Ltd.
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

#include <Common/AlignedBuffer.h>
#include <Common/Exception.h>

#include <cassert>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_ALLOCATE_MEMORY;
}

void AlignedBuffer::alloc(size_t size, size_t alignment)
{
    void * new_buf;
    int res = ::posix_memalign(&new_buf, std::max(alignment, sizeof(void *)), size);
    if (0 != res)
        throwFromErrno(
            fmt::format("Cannot allocate memory (posix_memalign), size: {}, alignment: {}.", size, alignment),
            ErrorCodes::CANNOT_ALLOCATE_MEMORY,
            res);
    assert(buf == nullptr);
    buf = new_buf;
}

void AlignedBuffer::dealloc()
{
    ::free(buf); //NOLINT
}

void AlignedBuffer::reset(size_t size, size_t alignment)
{
    dealloc();
    alloc(size, alignment);
}

AlignedBuffer::AlignedBuffer(size_t size, size_t alignment)
{
    alloc(size, alignment);
}

AlignedBuffer::~AlignedBuffer()
{
    dealloc();
}

} // namespace DB
