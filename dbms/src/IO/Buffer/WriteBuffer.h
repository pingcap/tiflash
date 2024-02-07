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

#include <Common/Exception.h>
#include <IO/Buffer/BufferBase.h>

#include <algorithm>
#include <cstring>
#include <iostream>
#include <memory>


namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
}


/** A simple abstract class for buffered data writing (char sequences) somewhere.
  * Unlike std::ostream, it provides access to the internal buffer,
  *  and also allows you to manually manage the position inside the buffer.
  *
  * The successors must implement the nextImpl() method.
  */
class WriteBuffer : public BufferBase
{
public:
    WriteBuffer(Position ptr, size_t size)
        : BufferBase(ptr, size, 0)
    {}
    void set(Position ptr, size_t size) { BufferBase::set(ptr, size, 0); }

    /** write the data in the buffer (from the beginning of the buffer to the current position);
      * set the position to the beginning; throw an exception, if something is wrong
      */
    inline void next()
    {
        if (!offset())
            return;
        bytes += offset();

        try
        {
            nextImpl();
        }
        catch (...)
        {
            /** If the nextImpl() call was unsuccessful, move the cursor to the beginning,
              * so that later (for example, when the stack was expanded) there was no second attempt to write data.
              */
            pos = working_buffer.begin();
            throw;
        }

        pos = working_buffer.begin();
    }

    /** it is desirable in the successors to place the next() call in the destructor,
      * so that the last data is written
      */
    virtual ~WriteBuffer() = default;

    inline void nextIfAtEnd()
    {
        if (!hasPendingData())
            next();
    }


    void write(const char * from, size_t n)
    {
        size_t bytes_copied = 0;

        while (bytes_copied < n)
        {
            nextIfAtEnd();
            size_t bytes_to_copy = std::min(static_cast<size_t>(working_buffer.end() - pos), n - bytes_copied);
            std::memcpy(pos, from + bytes_copied, bytes_to_copy);
            pos += bytes_to_copy;
            bytes_copied += bytes_to_copy;
        }
    }

    template <class T>
    __attribute__((always_inline)) void writeFixed(const T * __restrict from)
    {
        if (likely(working_buffer.end() - pos >= static_cast<ptrdiff_t>(sizeof(T))))
        {
            tiflash_compiler_builtin_memcpy(pos, from, sizeof(T));
            pos += sizeof(T);
        }
        else
        {
            [&]() __attribute__((noinline))
            {
                write(reinterpret_cast<const char *>(from), sizeof(T));
            }
            ();
        }
    }


    inline void write(char x)
    {
        nextIfAtEnd();
        *pos = x;
        ++pos;
    }

private:
    /** Write the data in the buffer (from the beginning of the buffer to the current position).
      * Throw an exception if something is wrong.
      */
    virtual void nextImpl()
    {
        throw Exception("Cannot write after end of buffer.", ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER);
    };
};


using WriteBufferPtr = std::shared_ptr<WriteBuffer>;


} // namespace DB
