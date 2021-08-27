#pragma once
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <streambuf>

namespace DB
{
namespace _Impl
{
class OutputBufferWrapper : public std::streambuf
{
    WriteBuffer & underlying;

public:
    explicit OutputBufferWrapper(WriteBuffer & underlying)
        : underlying(underlying)
    {
        underlying.next();
        auto gptr = underlying.buffer().begin();
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
            auto gptr = underlying.buffer().begin();
            this->setg(gptr, gptr, underlying.buffer().end());
        }
        return this->gptr() == this->egptr() ? std::char_traits<char>::eof() : std::char_traits<char>::to_int_type(*this->gptr());
    }
};

struct InputStreamWrapperBase
{
    _Impl::InputBufferWrapper buffer_wrapper;
    explicit InputStreamWrapperBase(ReadBuffer & underlying)
        : buffer_wrapper(underlying)
    {}
};

struct OutputStreamWrapperBase
{
    _Impl::OutputBufferWrapper buffer_wrapper;
    explicit OutputStreamWrapperBase(WriteBuffer & underlying)
        : buffer_wrapper(underlying)
    {}
};


} // namespace _Impl

// the problem here is that `std::ios` is the base class determining the buffer, but it is hard to
// construct a buffer before `std::ios` unless we set the virtual base for the wrapper. By doing so,
// we can construct the virtual base first and then pass the buffer to `std::ios`.
class InputStreamWrapper : virtual _Impl::InputStreamWrapperBase
    , public std::istream
{
public:
    explicit InputStreamWrapper(ReadBuffer & underlying)
        : _Impl::InputStreamWrapperBase(underlying)
        , std::ios(&this->buffer_wrapper)
        , std::istream(&this->buffer_wrapper)
    {}
};

class OutputStreamWrapper : virtual _Impl::OutputStreamWrapperBase
    , public std::ostream
{
public:
    explicit OutputStreamWrapper(WriteBuffer & underlying)
        : _Impl::OutputStreamWrapperBase(underlying)
        , std::ios(&this->buffer_wrapper)
        , std::ostream(&this->buffer_wrapper)
    {}
};


} // namespace DB