#pragma once

#include <IO/ReadBuffer.h>

#include <cstddef>


namespace DB
{
/** Allows to read from another ReadBuffer no more than the specified number of bytes.
  */
class LimitReadBuffer : public ReadBuffer
{
private:
    ReadBuffer & in;
    size_t limit;

    bool nextImpl() override;

public:
    LimitReadBuffer(ReadBuffer & in_, size_t limit_);
    ~LimitReadBuffer() override;
};

} // namespace DB
