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

#include <IO/Buffer/ReadBufferFromFileBase.h>
#include <IO/Buffer/StdStreamBufFromReadBuffer.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int SEEK_POSITION_OUT_OF_BOUND;
} // namespace ErrorCodes


StdStreamBufFromReadBuffer::StdStreamBufFromReadBuffer(std::unique_ptr<ReadBuffer> read_buffer_, size_t size_)
    : read_buffer(std::move(read_buffer_))
    , seekable_read_buffer(dynamic_cast<ReadBufferFromFileBase *>(read_buffer.get()))
    , size(size_)
{
    // setg() set the get area pointers of Base class
    this->setg(read_buffer->buffer().begin(), read_buffer->buffer().begin(), read_buffer->buffer().end());
}

StdStreamBufFromReadBuffer::StdStreamBufFromReadBuffer(ReadBuffer & read_buffer_, size_t size_)
    : size(size_)
{
    if (dynamic_cast<ReadBufferFromFileBase *>(&read_buffer_))
    {
        read_buffer = wrapReadBufferReference(static_cast<ReadBufferFromFileBase &>(read_buffer_));
        seekable_read_buffer = static_cast<ReadBufferFromFileBase *>(read_buffer.get());
    }
    else
    {
        read_buffer = wrapReadBufferReference(read_buffer_);
    }
    // setg() set the get area pointers of Base class
    this->setg(read_buffer->buffer().begin(), read_buffer->buffer().begin(), read_buffer->buffer().end());
}

StdStreamBufFromReadBuffer::~StdStreamBufFromReadBuffer() = default;

int StdStreamBufFromReadBuffer::underflow()
{
    // the current position may be changed in functions of Base class, so we need to update it
    read_buffer->position() = this->gptr();
    char c;
    bool success = read_buffer->peek(c);
    // the get area will be changed after peek(), so we need to update pointers of Base class
    this->setg(read_buffer->buffer().begin(), read_buffer->position(), read_buffer->buffer().end());
    if (!success)
        return std::char_traits<char>::eof();
    return c;
}

std::streamsize StdStreamBufFromReadBuffer::showmanyc()
{
    return read_buffer->available();
}

std::streamsize StdStreamBufFromReadBuffer::xsgetn(char_type * s, std::streamsize count)
{
    read_buffer->position() = this->gptr();
    size_t ret = read_buffer->read(s, count);
    this->setg(read_buffer->buffer().begin(), read_buffer->position(), read_buffer->buffer().end());
    return ret;
}

std::streampos StdStreamBufFromReadBuffer::seekoff(
    std::streamoff off,
    std::ios_base::seekdir dir,
    std::ios_base::openmode which)
{
    if (dir == std::ios_base::beg)
        return seekpos(off, which);
    else if (dir == std::ios_base::cur)
        return seekpos(getCurrentPosition() + off, which);
    else if (dir == std::ios_base::end)
        return seekpos(size + off, which);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong seek's base {}", static_cast<int>(dir));
}

std::streampos StdStreamBufFromReadBuffer::seekpos(std::streampos pos, std::ios_base::openmode which)
{
    if (!(which & std::ios_base::in))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong seek mode {}", static_cast<int>(which));

    read_buffer->position() = this->gptr();
    std::streamoff offset = pos - getCurrentPosition();
    if (offset == 0)
        return pos;

    if ((read_buffer->buffer().begin() <= read_buffer->position() + offset)
        && (read_buffer->position() + offset <= read_buffer->buffer().end()))
    {
        read_buffer->position() += offset;
        this->setg(read_buffer->buffer().begin(), read_buffer->position(), read_buffer->buffer().end());
        return pos;
    }

    if (seekable_read_buffer)
    {
        size_t ret = seekable_read_buffer->seek(pos, SEEK_SET);
        this->setg(read_buffer->buffer().begin(), read_buffer->position(), read_buffer->buffer().end());
        return ret;
    }

    if (offset > 0)
    {
        read_buffer->ignore(offset);
        this->setg(read_buffer->buffer().begin(), read_buffer->position(), read_buffer->buffer().end());
        return pos;
    }

    throw Exception(
        ErrorCodes::SEEK_POSITION_OUT_OF_BOUND,
        "Seek's offset {} is out of bound",
        static_cast<UInt64>(pos));
}

std::streampos StdStreamBufFromReadBuffer::getCurrentPosition() const
{
    if (seekable_read_buffer)
        return seekable_read_buffer->getPositionInFile();
    else
        return read_buffer->count();
}

std::streamsize StdStreamBufFromReadBuffer::xsputn(const char *, std::streamsize)
{
    throw Exception("StdStreamBufFromReadBuffer cannot be used for output", ErrorCodes::LOGICAL_ERROR);
}

int StdStreamBufFromReadBuffer::overflow(int)
{
    throw Exception("StdStreamBufFromReadBuffer cannot be used for output", ErrorCodes::LOGICAL_ERROR);
}

} // namespace DB
