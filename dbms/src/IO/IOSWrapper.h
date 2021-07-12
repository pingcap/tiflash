//
// Created by schrodinger on 7/12/21.
//

#ifndef CLICKHOUSE_IOSWRAPPER_H
#define CLICKHOUSE_IOSWRAPPER_H
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <streambuf>

namespace DB
{

namespace detail
{
class OutputBufferWrapper : public std::streambuf
{
    WriteBuffer & underlying;

public:
    explicit OutputBufferWrapper(WriteBuffer & underlying) : underlying(underlying)
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
    // so we should not call `underlying.next()` each time a sync is issued, which will break the efficiency of buffer
    // (an even evil problem is that some buffer like CH's `WriteBufferFromVector` doubles the space on each call
    // of `next()`. P.S. maybe this is a bug? see dbms/src/IO/WriteBufferFromVector.h:40).
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
    explicit InputBufferWrapper(ReadBuffer & underlying) : underlying(underlying)
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
    detail::InputBufferWrapper bufferWrapper;
    explicit InputStreamWrapperBase(ReadBuffer & underlying) : bufferWrapper(underlying) {}
};

struct OutputStreamWrapperBase
{
    detail::OutputBufferWrapper bufferWrapper;
    explicit OutputStreamWrapperBase(WriteBuffer & underlying) : bufferWrapper(underlying) {}
};


} // namespace detail

// the problem here is that `std::ios` is the base class determining the buffer, but it is hard to
// construct a buffer before `std::ios` unless we set the virtual base for the wrapper. By doing so,
// we can construct the virtual base first and then pass the buffer to `std::ios`.
class InputStreamWrapper : virtual detail::InputStreamWrapperBase, public std::istream
{
public:
    explicit InputStreamWrapper(ReadBuffer & underlying)
        : detail::InputStreamWrapperBase(underlying), std::ios(&this->bufferWrapper), std::istream(&this->bufferWrapper)
    {}
};

class OutputStreamWrapper : virtual detail::OutputStreamWrapperBase, public std::ostream
{
public:
    explicit OutputStreamWrapper(WriteBuffer & underlying)
        : detail::OutputStreamWrapperBase(underlying), std::ios(&this->bufferWrapper), std::ostream(&this->bufferWrapper)
    {}
};


} // namespace DB


#endif //CLICKHOUSE_IOSWRAPPER_H
