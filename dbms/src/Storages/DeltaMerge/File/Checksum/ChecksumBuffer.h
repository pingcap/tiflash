//
// Created by schrodinger on 7/5/21.
//

#ifndef CLICKHOUSE_CHECKSUMBUFFER_H
#define CLICKHOUSE_CHECKSUMBUFFER_H

#ifndef TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE
#define TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE (1u << 15u)
#endif // TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE

#include <Encryption/FileProvider.h>
#include <IO/WriteBufferFromFileBase.h>

#include "Checksum.h"

namespace DB::DM::Checksum
{


/*!
 * A frame consists of a header and a body that conforms the following structure:
 *
 * <pre>{@code
 * ---------------------------------
 * | > header                      |
 * |   - bytes                     |
 * |   - checksum                  |
 * ---------------------------------
 * | > data (size = header.bytes)  |
 * |             ...               |
 * |             ...               |
 * |             ...               |
 * ---------------------------------
 * }</pre>
 *
 * When writing a frame, we maintain the buffer than is of the exact size of the data part.
 * Whenever the buffer is full, we digest the whole buffer and update the header info, write back
 * the header and the body to the underlying file.
 * A special case is the last frame where the body can be less than the buffer size; but no special
 * handling of it is needed since the body length is already recorded in the header.
 *
 * The `FramedChecksumWriteBuffer` should be used directly on the file; the stream's ending has no
 * special mark: that is it ends when the file reaches EOF mark.
 */
template <typename Backend>
class FramedChecksumWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
private:
    WritableFilePtr out;
    void            nextImpl() override
    {
        size_t len   = this->offset();
        auto & frame = reinterpret_cast<ChecksumFrame<Backend> &>(
            *(this->working_buffer.begin() - sizeof(ChecksumFrame<Backend>))); // align should not fail
        frame.bytes = len;
        auto digest = Backend{};
        digest.update(frame.data, frame.bytes);
        frame.checksum = digest.checksum();

        auto iter     = this->working_buffer.begin() - sizeof(ChecksumFrame<Backend>);
        auto expected = len + sizeof(ChecksumFrame<Backend>);

        while (expected != 0)
        {
            auto count = out->write(iter, expected);
            if (unlikely(count == -1))
            {
                if (errno == EINTR)
                    continue;
                else
                {
                    // TODO: change throw behavior
                    throw Poco::WriteFileException("failed to flush data to file", -errno);
                }
            }
            iter += count;
            expected -= count;
        }
    }

public:
    explicit FramedChecksumWriteBuffer(WritableFilePtr out_, size_t block_size_ = TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE)
        : BufferWithOwnMemory<WriteBuffer>(sizeof(ChecksumFrame<Backend>) + block_size_ + 512, nullptr, alignof(ChecksumFrame<Backend>)),
          out(std::move(out_))
    {
        // adjust alignment, aligned memory boundary can make it fast for digesting
        auto shifted = this->working_buffer.begin() + sizeof(ChecksumFrame<Backend>);
        auto offset  = (-reinterpret_cast<uintptr_t>(shifted)) & (512u - 1u);
        auto result  = this->working_buffer.begin() + offset;
        set(result + sizeof(ChecksumFrame<Backend>), block_size_);
        position() = working_buffer.begin(); // empty the buffer
    }

    ~FramedChecksumWriteBuffer() override { next(); }
};

/*!
 * Similar to the `FramedChecksumWriteBuffer`, the reading buffer also directly operates on the file.
 * It keeps a buffer with the length of header plus maximum body length.
 *
 * Everytime when the buffer is exhausted (or at the beginning state), the reader will first try loading
 * the header; then according to the length information in the header, the body part will also be loaded
 * from the file. The the reading operation succeeds, the whole body will be digested to generate the
 * checksum, which is then compared with the checksum info in the header.
 *
 * Working together with the writing buffer, also read and write operations should be as normal as regular
 * reading buffers. The checking part is hidden in the background, and an exception will thrown if anything
 * bad is detected.
 */
template <typename Backend>
class FramedChecksumReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
    explicit FramedChecksumReadBuffer(RandomAccessFilePtr in_, size_t block_size = TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE)
        : BufferWithOwnMemory<ReadBuffer>(sizeof(ChecksumFrame<Backend>) + block_size + 512, nullptr, alignof(ChecksumFrame<Backend>)),
          in(std::move(in_))
    {
        // adjust alignment, aligned memory boundary can make it fast for digesting
        auto shifted = this->working_buffer.begin() + sizeof(ChecksumFrame<Backend>);
        auto offset  = (-reinterpret_cast<uintptr_t>(shifted)) & (512u - 1u);
        auto result  = this->working_buffer.begin() + offset;
        set(result, sizeof(ChecksumFrame<Backend>) + block_size);
    }

private:
    size_t expectRead(Position pos, size_t size)
    {
        size_t expected = size;
        while (expected != 0)
        {
            auto count = in->read(pos, expected);
            if (count == 0)
            {
                break;
            }
            if (unlikely(count < 0))
            {
                if (errno == EINTR)
                    continue;
                else
                {
                    // TODO: change throw behavior
                    throw Poco::ReadFileException("failed to read data from file", -errno);
                }
            }
            expected -= count;
            pos += count;
        }
        return size - expected;
    }

    bool nextImpl() override
    {
        // first, read frame header;
        auto & frame  = reinterpret_cast<ChecksumFrame<Backend> &>(*this->working_buffer.begin()); // align should not fail
        auto   length = expectRead(working_buffer.begin(), sizeof(ChecksumFrame<Backend>));
        if (length == 0)
            return false; // EOF
        if (unlikely(length != sizeof(ChecksumFrame<Backend>)))
        {
            throwReadAfterEOF();
        }

        // now read the body
        length = expectRead(reinterpret_cast<Position>(frame.data), frame.bytes);
        if (unlikely(length != frame.bytes))
        {
            throwReadAfterEOF();
        }

        // examine checksum
        auto digest = Backend{};
        digest.update(frame.data, frame.bytes);
        if (unlikely(frame.checksum != digest.checksum()))
        {
            // TODO: change throw behavior
            throw Poco::ReadFileException("file corruption detected", -errno);
        }

        //shift position
        working_buffer_offset = sizeof(frame);
        working_buffer.resize(sizeof(frame) + length);
        return true;
    }

    RandomAccessFilePtr in;
};

} // namespace DB::DM::Checksum


#endif //CLICKHOUSE_CHECKSUMBUFFER_H
