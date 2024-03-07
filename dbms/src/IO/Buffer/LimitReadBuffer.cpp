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

#include <IO/Buffer/LimitReadBuffer.h>


namespace DB
{
bool LimitReadBuffer::nextImpl()
{
    /// Let underlying buffer calculate read bytes in `next()` call.
    in.position() = position();

    if (bytes >= limit || !in.next())
        return false;

    working_buffer = in.buffer();

    if (limit - bytes < working_buffer.size())
        working_buffer.resize(limit - bytes);

    return true;
}


LimitReadBuffer::LimitReadBuffer(ReadBuffer & in_, size_t limit_)
    : ReadBuffer(in_.position(), 0)
    , in(in_)
    , limit(limit_)
{
    size_t remaining_bytes_in_buffer = in.buffer().end() - in.position();
    if (remaining_bytes_in_buffer > limit)
        remaining_bytes_in_buffer = limit;

    working_buffer = Buffer(in.position(), in.position() + remaining_bytes_in_buffer);
}


LimitReadBuffer::~LimitReadBuffer()
{
    /// Update underlying buffer's position in case when limit wasn't reached.
    if (working_buffer.size() != 0)
        in.position() = position();
}

} // namespace DB
