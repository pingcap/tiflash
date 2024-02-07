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
#include <IO/Buffer/ReadBuffer.h>

#include <iostream>


namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_READ_FROM_ISTREAM;
}


class ReadBufferFromIStream : public BufferWithOwnMemory<ReadBuffer>
{
private:
    std::istream & istr;

    bool nextImpl() override
    {
        istr.read(internal_buffer.begin(), internal_buffer.size());
        size_t gcount = istr.gcount();

        if (!gcount)
        {
            if (istr.eof())
                return false;
            else
                throw Exception("Cannot read from istream", ErrorCodes::CANNOT_READ_FROM_ISTREAM);
        }
        else
            working_buffer.resize(gcount);

        return true;
    }

public:
    ReadBufferFromIStream(std::istream & istr_, size_t size = DBMS_DEFAULT_BUFFER_SIZE)
        : BufferWithOwnMemory<ReadBuffer>(size)
        , istr(istr_)
    {}
};

} // namespace DB
