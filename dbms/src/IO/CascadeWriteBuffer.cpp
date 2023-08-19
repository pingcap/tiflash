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

#include <Common/Exception.h>
#include <IO/CascadeWriteBuffer.h>

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
extern const int CANNOT_CREATE_IO_BUFFER;
} // namespace ErrorCodes

CascadeWriteBuffer::CascadeWriteBuffer(WriteBufferPtrs && prepared_sources_, WriteBufferConstructors && lazy_sources_)
    : WriteBuffer(nullptr, 0)
    , prepared_sources(std::move(prepared_sources_))
    , lazy_sources(std::move(lazy_sources_))
{
    first_lazy_source_num = prepared_sources.size();
    num_sources = first_lazy_source_num + lazy_sources.size();

    /// fill lazy sources by nullptr
    prepared_sources.resize(num_sources);

    curr_buffer_num = 0;
    curr_buffer = setNextBuffer();
    set(curr_buffer->buffer().begin(), curr_buffer->buffer().size());
}


void CascadeWriteBuffer::nextImpl()
{
    try
    {
        curr_buffer->position() = position();
        curr_buffer->next();
    }
    catch (const Exception & e)
    {
        if (curr_buffer_num < num_sources && e.code() == ErrorCodes::CURRENT_WRITE_BUFFER_IS_EXHAUSTED)
        {
            /// TODO: protocol should require set(position(), 0) before Exception

            /// good situation, fetch next WriteBuffer
            ++curr_buffer_num;
            curr_buffer = setNextBuffer();
        }
        else
            throw;
    }

    set(curr_buffer->position(), curr_buffer->buffer().end() - curr_buffer->position());
}


void CascadeWriteBuffer::getResultBuffers(WriteBufferPtrs & res)
{
    /// Sync position with underlying buffer before invalidating
    curr_buffer->position() = position();

    res = std::move(prepared_sources);

    curr_buffer = nullptr;
    curr_buffer_num = num_sources = 0;
    prepared_sources.clear();
    lazy_sources.clear();
}


WriteBuffer * CascadeWriteBuffer::setNextBuffer()
{
    if (first_lazy_source_num <= curr_buffer_num && curr_buffer_num < num_sources)
    {
        if (!prepared_sources[curr_buffer_num])
        {
            WriteBufferPtr prev_buf = (curr_buffer_num > 0) ? prepared_sources[curr_buffer_num - 1] : nullptr;
            prepared_sources[curr_buffer_num] = lazy_sources[curr_buffer_num - first_lazy_source_num](prev_buf);
        }
    }
    else if (curr_buffer_num >= num_sources)
        throw Exception("There are no WriteBuffers to write result", ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER);

    WriteBuffer * res = prepared_sources[curr_buffer_num].get();
    if (!res)
        throw Exception("Required WriteBuffer is not created", ErrorCodes::CANNOT_CREATE_IO_BUFFER);

    /// Check that returned buffer isn't empty
    if (!res->hasPendingData())
        res->next();

    return res;
}


CascadeWriteBuffer::~CascadeWriteBuffer()
{
    /// Sync position with underlying buffer before exit
    if (curr_buffer)
        curr_buffer->position() = position();
}


} // namespace DB
