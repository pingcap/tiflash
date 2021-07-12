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
        if (this->gptr() == this->egptr())
        {
            underlying.position() = this->gptr();
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
