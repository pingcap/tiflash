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


/** Computes the hash from the data to write and passes it to the specified WriteBuffer.
  * The buffer of the nested WriteBuffer is used as the main buffer.
  */
template <typename Backend>
class FramedChecksumWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
private:
    WritableFilePtr out;
    void            nextImpl() override
    {
        size_t len = this->offset();
        if (len <= sizeof(ChecksumFrame<Backend>))
            return; // skip empty frame

        auto & frame = reinterpret_cast<ChecksumFrame<Backend> &>(*this->working_buffer.begin()); // align should not fail
        frame.bytes  = len - sizeof(ChecksumFrame<Backend>);
        auto digest  = Backend{};
        digest.update(frame.data, frame.bytes);
        frame.checksum = digest.checksum();

        auto iter     = this->working_buffer.begin();
        auto expected = len;

        while (expected != 0)
        {
            auto count = out->write(iter, len - expected);
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

        position() = reinterpret_cast<Position>(frame.data); // shift frame
    }

public:
    using HashType = typename Backend::HashType;

    explicit FramedChecksumWriteBuffer(WritableFilePtr out_, size_t block_size_ = TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE)
        : BufferWithOwnMemory<WriteBuffer>(sizeof(ChecksumFrame<Backend>) + block_size_ + 512, nullptr, alignof(ChecksumFrame<Backend>)),
          out(std::move(out_))
    {
        // adjust alignment, aligned memory boundary can makes it fast for digesting
        auto shifted = this->working_buffer.begin() + sizeof(ChecksumFrame<Backend>);
        auto offset  = (-reinterpret_cast<uintptr_t>(shifted)) & (512u - 1u);
        auto result  = this->working_buffer.begin() + offset;
        set(result, sizeof(ChecksumFrame<Backend>) + block_size_);
    }

    ~FramedChecksumWriteBuffer() override { next(); }
};

/*
 * Calculates the hash from the read data. When reading, the data is read from the nested ReadBuffer.
 * Small pieces are copied into its own memory.
 */
template <typename Backend>
class FramedChecksumReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
    explicit FramedChecksumReadBuffer(RandomAccessFilePtr in_, size_t block_size = TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE)
        : BufferWithOwnMemory<ReadBuffer>(sizeof(ChecksumFrame<Backend>) + block_size + 512, nullptr, alignof(ChecksumFrame<Backend>)),
          in(std::move(in_))
    {
        // adjust alignment, aligned memory boundary can makes it fast for digesting
        auto shifted = this->working_buffer.begin() + sizeof(ChecksumFrame<Backend>);
        auto offset  = (-reinterpret_cast<uintptr_t>(shifted)) & (512u - 1u);
        auto result  = this->working_buffer.begin() + offset;
        set(result, sizeof(ChecksumFrame<Backend>) + block_size);
    }

private:
    size_t expectRead(Position pos, size_t size) {
        size_t expected = size;
        while (expected != 0) {
            auto count = in->read(pos, size);
            if (count == 0) {
                break;
            }
            if (unlikely(count < 0)) {
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
        auto & frame = reinterpret_cast<ChecksumFrame<Backend> &>(*this->working_buffer.begin()); // align should not fail
        auto length = expectRead(working_buffer.begin(), sizeof(ChecksumFrame<Backend>));
        if (length == 0) return false; // EOF
        if (unlikely(length != sizeof(ChecksumFrame<Backend>))) {
            throwReadAfterEOF();
        }

        // now read the body
        length = expectRead(reinterpret_cast<Position>(frame.data), frame.bytes);
        if (unlikely(length != frame.bytes)) {
            throwReadAfterEOF();
        }

        // examine checksum
        auto digest = Backend {};
        digest.update(frame.data, frame.bytes);
        if (unlikely(frame.checksum != digest.checksum())) {
            // TODO: change throw behavior
            throw Poco::ReadFileException("file corruption detected", -errno);
        }

        //shift position
        position() = reinterpret_cast<Position>(frame.data);

        return true;
    }

    RandomAccessFilePtr in;
};

} // namespace DB::DM::Checksum


#endif //CLICKHOUSE_CHECKSUMBUFFER_H
