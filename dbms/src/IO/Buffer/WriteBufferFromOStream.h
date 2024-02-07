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
#include <IO/Buffer/BufferWithOwnMemory.h>
#include <IO/Buffer/WriteBuffer.h>

#include <iostream>


namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_WRITE_TO_OSTREAM;
}

class WriteBufferFromOStream : public BufferWithOwnMemory<WriteBuffer>
{
private:
    std::ostream & ostr;

    void nextImpl() override
    {
        if (!offset())
            return;

        ostr.write(working_buffer.begin(), offset());
        ostr.flush();

        if (!ostr.good())
            throw Exception("Cannot write to ostream", ErrorCodes::CANNOT_WRITE_TO_OSTREAM);
    }

public:
    WriteBufferFromOStream(
        std::ostream & ostr_,
        size_t size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0)
        : BufferWithOwnMemory<WriteBuffer>(size, existing_memory, alignment)
        , ostr(ostr_)
    {}

    ~WriteBufferFromOStream() override
    {
        try
        {
            next();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
};

} // namespace DB
