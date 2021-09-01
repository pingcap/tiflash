#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressedReadBufferBase.h>
#include <IO/ReadBuffer.h>


namespace DB
{
template <bool has_checksum = true>
class CompressedReadBuffer : public CompressedReadBufferBase<has_checksum>
    , public BufferWithOwnMemory<ReadBuffer>
{
private:
    size_t size_compressed = 0;

    bool nextImpl() override;

public:
    CompressedReadBuffer(ReadBuffer & in_)
        : CompressedReadBufferBase<has_checksum>(&in_)
        , BufferWithOwnMemory<ReadBuffer>(0)
    {}

    size_t readBig(char * to, size_t n) override;

    /// The compressed size of the current block.
    size_t getSizeCompressed() const { return size_compressed; }
};

} // namespace DB
