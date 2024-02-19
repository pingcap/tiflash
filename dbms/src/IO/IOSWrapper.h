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
#include <IO/Buffer/WriteBuffer.h>

#include <streambuf>

namespace DB
{
namespace Detail
{
class OutputBufferWrapper : public std::streambuf
{
    WriteBuffer & underlying;

public:
    explicit OutputBufferWrapper(WriteBuffer & underlying)
        : underlying(underlying)
    {
        underlying.next();
        auto * gptr = underlying.buffer().begin();
        this->setg(gptr, gptr, gptr);
    }

    int overflow(int_type c) override
    {
        this->sync();
        if (c != std::char_traits<char>::eof())
        {
            underlying.write(static_cast<char>(c));
            this->setg(underlying.buffer().begin(), underlying.position(), underlying.buffer().end());
        }
        return c;
    }

    // STL's stream seems to call sync/overflow in a very eager manner (i.e. `std::endl` will trigger the sync);
    // so we should not call `underlying.next()` each time a sync is issued, which will break the efficiency of buffer.
    // Anyway, by updating the position, we can already make sure the update to notified to the underlying buffer.
    // This is also more friendly for compressing or checksum framing.
    int sync() override
    {
        underlying.position() = this->gptr();
        underlying.nextIfAtEnd();
        this->setg(underlying.buffer().begin(), underlying.position(), underlying.buffer().end());
        return 0;
    }
};

class InputBufferWrapper : public std::streambuf
{
    ReadBuffer & underlying;

public:
    explicit InputBufferWrapper(ReadBuffer & underlying)
        : underlying(underlying)
    {
        this->setg(underlying.buffer().begin(), underlying.position(), underlying.buffer().end());
    }

    int underflow() override
    {
        underlying.position() = this->gptr();
        if (this->gptr() == this->egptr())
        {
            underlying.next();
            auto * gptr = underlying.buffer().begin();
            this->setg(gptr, gptr, underlying.buffer().end());
        }
        return this->gptr() == this->egptr() ? std::char_traits<char>::eof()
                                             : std::char_traits<char>::to_int_type(*this->gptr());
    }
};

struct InputStreamWrapperBase
{
    Detail::InputBufferWrapper buffer_wrapper;
    explicit InputStreamWrapperBase(ReadBuffer & underlying)
        : buffer_wrapper(underlying)
    {}
};

struct OutputStreamWrapperBase
{
    Detail::OutputBufferWrapper buffer_wrapper;
    explicit OutputStreamWrapperBase(WriteBuffer & underlying)
        : buffer_wrapper(underlying)
    {}
};


} // namespace Detail

// the problem here is that `std::ios` is the base class determining the buffer, but it is hard to
// construct a buffer before `std::ios` unless we set the virtual base for the wrapper. By doing so,
// we can construct the virtual base first and then pass the buffer to `std::ios`.

/// InputStreamWrapper
/// Other C++ libraries may rely on \code{.cpp}std::istream\endcode or \code{.cpp}std::ostream\endcode
/// to do IO operations; hence we provide this layer to allow users to direct IO to/from our buffers.
/// \example
/// \code{.cpp}
/// ReadBuffer & tiflash_buffer = ...;
/// InputStreamWrapper istream{ tiflash_buffer };
/// proto.ParseFromIstream( istream );
/// \endcode
class InputStreamWrapper
    : virtual Detail::InputStreamWrapperBase
    , public std::istream
{
public:
    explicit InputStreamWrapper(ReadBuffer & underlying)
        : Detail::InputStreamWrapperBase(underlying)
        , std::ios(&this->buffer_wrapper)
        , std::istream(&this->buffer_wrapper)
    {}
};

/// OutputStreamWrapper
/// \example
/// \code{.cpp}
/// WriteBuffer & tiflash_buffer = ...;
/// OutputStreamWrapper ostream{ tiflash_buffer };
/// proto.SerializeToOstream( ostream );
/// \endcode
class OutputStreamWrapper
    : virtual Detail::OutputStreamWrapperBase
    , public std::ostream
{
public:
    explicit OutputStreamWrapper(WriteBuffer & underlying)
        : Detail::OutputStreamWrapperBase(underlying)
        , std::ios(&this->buffer_wrapper)
        , std::ostream(&this->buffer_wrapper)
    {}
};


} // namespace DB
