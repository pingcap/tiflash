#pragma once

#include <IO/WriteBuffer.h>

namespace DB
{
/*!
 * WriteBufferProxy class. This class provides a transparent layer above the underlying buffer (no extra memcpy and memory allocation).
 * One can utilize this layer to get precise bytes count and other statistics.
 */
class WriteBufferProxy : public DB::WriteBuffer
{
private:
    WriteBuffer & out;

    void nextImpl() override
    {
        out.position() = pos;
        out.next();
        working_buffer = out.buffer();
    }

public:
    explicit WriteBufferProxy(DB::WriteBuffer & out_)
        : DB::WriteBuffer(nullptr, 0)
        , out(out_)
    {
        out.next(); /// flush away data before us.
        working_buffer = out.buffer();
        pos = working_buffer.begin();
    }
};
} // namespace DB
