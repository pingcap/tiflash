#pragma once

#include <IO/ReadBuffer.h>

#include <vector>


namespace DB
{
/** Reads from the concatenation of multiple ReadBuffers
  */
class ConcatReadBufferWithPtr : public ReadBuffer
{
public:
    using ReadBuffers = std::vector<ReadBufferPtr>;

protected:
    ReadBuffers buffers;
    ReadBuffers::iterator current;

    bool nextImpl() override
    {
        if (buffers.end() == current)
            return false;

        /// First reading
        if (working_buffer.size() == 0 && (*current)->hasPendingData())
        {
            working_buffer = Buffer((*current)->position(), (*current)->buffer().end());
            return true;
        }

        if (!(*current)->next())
        {
            ++current;
            if (buffers.end() == current)
                return false;

            /// We skip the filled up buffers; if the buffer is not filled in, but the cursor is at the end, then read the next piece of data.
            while ((*current)->eof())
            {
                ++current;
                if (buffers.end() == current)
                    return false;
            }
        }

        working_buffer = Buffer((*current)->position(), (*current)->buffer().end());
        return true;
    }

public:
    ConcatReadBufferWithPtr(const ReadBuffers & buffers_)
        : ReadBuffer(nullptr, 0)
        , buffers(buffers_)
        , current(buffers.begin())
    {}
};

} // namespace DB
