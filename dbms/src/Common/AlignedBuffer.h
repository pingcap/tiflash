// Copyright 2024 PingCAP, Ltd.
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

#include <boost/noncopyable.hpp>
#include <cstdlib>
#include <utility>

namespace DB
{

/** Aligned piece of memory.
  * It can only be allocated and destroyed.
  * MemoryTracker is not used. AlignedBuffer is intended for small pieces of memory.
  */
class AlignedBuffer : private boost::noncopyable
{
private:
    void * buf = nullptr;

    void alloc(size_t size, size_t alignment);
    void dealloc();

public:
    AlignedBuffer() = default;
    AlignedBuffer(size_t size, size_t alignment);
    AlignedBuffer(AlignedBuffer && old) noexcept { std::swap(buf, old.buf); }
    ~AlignedBuffer();

    void reset(size_t size, size_t alignment);

    char * data() { return static_cast<char *>(buf); }
    const char * data() const { return static_cast<const char *>(buf); }
};

} // namespace DB
