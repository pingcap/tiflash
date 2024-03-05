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

#include <IO/Buffer/ReadBuffer.h>


namespace DB
{
/** Allows to read from memory range.
  * In comparison with just ReadBuffer, it only adds convenient constructors, that do const_cast.
  * In fact, ReadBuffer will not modify data in buffer, but it requires non-const pointer.
  */
class ReadBufferFromMemory : public ReadBuffer
{
public:
    ReadBufferFromMemory(const char * buf, size_t size)
        : ReadBuffer(const_cast<char *>(buf), size, 0)
    {}

    ReadBufferFromMemory(const unsigned char * buf, size_t size)
        : ReadBuffer(const_cast<char *>(reinterpret_cast<const char *>(buf)), size, 0)
    {}

    ReadBufferFromMemory(const signed char * buf, size_t size)
        : ReadBuffer(const_cast<char *>(reinterpret_cast<const char *>(buf)), size, 0)
    {}
};

} // namespace DB
