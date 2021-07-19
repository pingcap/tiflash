//
// Created by schrodinger on 7/5/21.
//

#ifndef CLICKHOUSE_CHECKSUMBUFFER_H
#define CLICKHOUSE_CHECKSUMBUFFER_H

#ifndef TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE
#define TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE DBMS_DEFAULT_BUFFER_SIZE
#endif // TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE

#include <Encryption/FileProvider.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>

#include "Checksum.h"

namespace DB::ErrorCodes
{
extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
extern const int CANNOT_FSYNC;
extern const int CANNOT_SEEK_THROUGH_FILE;
extern const int CANNOT_TRUNCATE_FILE;
extern const int ARGUMENT_OUT_OF_BOUND;
} // namespace DB::ErrorCodes
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
 *
 */


template <typename Backend>
class FramedChecksumWriteBuffer : public WriteBufferFromFileDescriptor
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

    off_t doSeek(off_t, int) override { throw Poco::FileException("framed file is not seekable in writing mode"); }


public:
    explicit FramedChecksumWriteBuffer(WritableFilePtr out_, size_t block_size_ = TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE)
        : WriteBufferFromFileDescriptor(
            out_->getFd(), sizeof(ChecksumFrame<Backend>) + block_size_ + 512, nullptr, alignof(ChecksumFrame<Backend>)),
          out(std::move(out_))
    {
        // adjust alignment, aligned memory boundary can make it fast for digesting
        std::memset(this->working_buffer.begin(), 0, sizeof(ChecksumFrame<Backend>) + block_size_ + 512);
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
class FramedChecksumReadBuffer : public ReadBufferFromFileDescriptor
{
public:
    explicit FramedChecksumReadBuffer(RandomAccessFilePtr in_,
                                      size_t              block_size    = TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE,
                                      bool                skipChecksum_ = false)
        : ReadBufferFromFileDescriptor(
            in_->getFd(), sizeof(ChecksumFrame<Backend>) + block_size + 512, nullptr, alignof(ChecksumFrame<Backend>)),
          frameSize(block_size),
          skipChecksum(skipChecksum_),
          in(std::move(in_))
    {
        // adjust alignment, aligned memory boundary can make it fast for digesting
        auto shifted = this->working_buffer.begin() + sizeof(ChecksumFrame<Backend>);
        auto offset  = (-reinterpret_cast<uintptr_t>(shifted)) & (512u - 1u);
        auto result  = this->working_buffer.begin() + offset;
        set(result + sizeof(ChecksumFrame<Backend>), block_size);
        // read at least one frame
        next();
        currentFrame = 0;
    }

    off_t getPositionInFile() override { return currentFrame * frameSize + offset(); }

private:
    size_t              currentFrame;
    const size_t        frameSize;
    const bool          skipChecksum;
    RandomAccessFilePtr in;
    size_t              expectRead(Position pos, size_t size)
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

    void readAndCheckBody()
    {
        auto & frame = reinterpret_cast<ChecksumFrame<Backend> &>(
            *(this->working_buffer.begin() - sizeof(ChecksumFrame<Backend>))); // align should not fail
        auto length = expectRead(reinterpret_cast<Position>(frame.data), frame.bytes);
        if (unlikely(length != frame.bytes))
        {
            throwReadAfterEOF();
        }

        // examine checksum
        if (!skipChecksum)
        {
            auto digest = Backend{};
            digest.update(frame.data, frame.bytes);
            if (unlikely(frame.checksum != digest.checksum()))
            {
                // TODO: change throw behavior
                throw Poco::ReadFileException("file corruption detected", -errno);
            }
        }

        // shift position, because the last frame may be of less length
        working_buffer.resize(length);
    }

    bool nextImpl() override
    {
        // first, read frame header
        auto length = expectRead(working_buffer.begin() - sizeof(ChecksumFrame<Backend>), sizeof(ChecksumFrame<Backend>));
        if (length == 0)
            return false; // EOF
        if (unlikely(length != sizeof(ChecksumFrame<Backend>)))
        {
            throwReadAfterEOF();
        }

        // now read the body
        readAndCheckBody();

        // update statistics
        currentFrame++;
        return true;
    }

    off_t doSeek(off_t offset, int whence) override
    {
        if (whence == SEEK_CUR)
        {
            offset = getPositionInFile() + offset;
        }
        else if (whence != SEEK_SET)
        {
            throw Poco::FileException("FramedChecksumReadBuffer::seek expects SEEK_SET or SEEK_CUR as whence",
                                      ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }
        auto targetFrame  = offset / frameSize;
        auto targetOffset = offset % frameSize;

        if (targetFrame == currentFrame)
        {
            pos = working_buffer.begin() + targetOffset;
            return offset;
        }
        else
        {
            // read the header
            auto headerOffset = targetFrame * (sizeof(ChecksumFrame<Backend>) + frameSize);
            auto result       = in->seek(static_cast<off_t>(headerOffset), SEEK_SET);
            if (result == -1)
            {
                throwFromErrno("Cannot seek through file " + in->getFileName(), ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
            }
            auto length = expectRead(working_buffer.begin() - sizeof(ChecksumFrame<Backend>), sizeof(ChecksumFrame<Backend>));
            if (length == 0 && targetOffset == 0)
            {
                currentFrame = targetFrame;
                pos          = working_buffer.begin();
                working_buffer.resize(0);
                return offset; // EOF
            }
            if (unlikely(length != sizeof(ChecksumFrame<Backend>)))
            {
                throwReadAfterEOF();
            }

            // read the body
            readAndCheckBody();

            // update statistics
            currentFrame = targetFrame;
            pos          = working_buffer.begin() + targetOffset;
        }

        return offset;
    }
};

} // namespace DB::DM::Checksum


#endif //CLICKHOUSE_CHECKSUMBUFFER_H
