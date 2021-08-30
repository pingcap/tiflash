#pragma once
#ifndef TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE
#define TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE DBMS_DEFAULT_BUFFER_SIZE
#endif // TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE

#include <Common/Checksum.h>
#include <Common/CurrentMetrics.h>
#include <Encryption/FileProvider.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <fmt/format.h>

namespace ProfileEvents
{
extern const Event ChecksumBufferRead;
extern const Event ChecksumBufferWrite;
extern const Event ChecksumBufferReadBytes;
extern const Event ChecksumBufferWriteBytes;
extern const Event ChecksumBufferSeek;
extern const Event Seek;
} // namespace ProfileEvents

namespace DB
{


/*
 * A frame consists of a header and a body that conforms the following structure:
 *
 *
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
 *
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
    size_t current_frame = 0;
    const size_t frame_size;
    void nextImpl() override
    {
        size_t len = this->offset();
        auto & frame = reinterpret_cast<ChecksumFrame<Backend> &>(
            *(this->working_buffer.begin() - sizeof(ChecksumFrame<Backend>))); // align should not fail
        frame.bytes = len;
        auto digest = Backend{};
        digest.update(frame.data, frame.bytes);
        frame.checksum = digest.checksum();

        auto iter = this->working_buffer.begin() - sizeof(ChecksumFrame<Backend>);
        auto expected = len + sizeof(ChecksumFrame<Backend>);

        while (expected != 0)
        {
            ProfileEvents::increment(ProfileEvents::ChecksumBufferWrite);

            ssize_t count;
            {
                count = out->write(iter, expected);
            }
            if (unlikely(count == -1))
            {
                if (errno == EINTR)
                    continue;
                else
                {
                    throw TiFlashException(fmt::format("cannot flush checksum framed data to {} (errno = {})", out->getFileName(), errno),
                        Errors::Checksum::IOFailure);
                }
            }
            iter += count;
            expected -= count;
        }

        ProfileEvents::increment(ProfileEvents::ChecksumBufferWriteBytes, len + sizeof(ChecksumFrame<Backend>));

        current_frame++;
    }

    off_t doSeek(off_t, int) override { throw Exception("framed file is not seekable in writing mode"); }

    off_t getPositionInFile() override { return current_frame * frame_size + offset(); }

public:
    explicit FramedChecksumWriteBuffer(WritableFilePtr out_, size_t block_size_ = TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE)
        : WriteBufferFromFileDescriptor(
            out_->getFd(), sizeof(ChecksumFrame<Backend>) + block_size_ + 512, nullptr, alignof(ChecksumFrame<Backend>)),
          out(std::move(out_)),
          frame_size(block_size_)
    {
        // adjust alignment, aligned memory boundary can make it fast for digesting
        auto shifted = this->working_buffer.begin() + sizeof(ChecksumFrame<Backend>);

        // offset is the distance to a nearest aligned boundary, the calculation follows the following
        // properties:
        //   1. (-x) & (alignment - 1) == (-x) % alignment     [power of 2]
        //   2. alignment - x % alignment == (-x) % alignment  [congruence property]
        auto offset = (-reinterpret_cast<uintptr_t>(shifted)) & (512u - 1u);
        auto result = this->working_buffer.begin() + offset;
        set(result + sizeof(ChecksumFrame<Backend>), block_size_);
        position() = working_buffer.begin(); // empty the buffer

        // avoid complaining about uninitialized bytes in the header area
        std::memset(this->working_buffer.begin() - sizeof(ChecksumFrame<Backend>), 0, sizeof(ChecksumFrame<Backend>));
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
    explicit FramedChecksumReadBuffer(
        RandomAccessFilePtr in_, size_t block_size = TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE, bool skip_checksum_ = false)
        : ReadBufferFromFileDescriptor(
            in_->getFd(), sizeof(ChecksumFrame<Backend>) + block_size + 512, nullptr, alignof(ChecksumFrame<Backend>)),
          frame_size(block_size),
          skip_checksum(skip_checksum_),
          in(std::move(in_))
    {
        // adjust alignment, aligned memory boundary can make it fast for digesting
        auto shifted = this->working_buffer.begin() + sizeof(ChecksumFrame<Backend>);
        auto offset = (-reinterpret_cast<uintptr_t>(shifted)) & (512u - 1u);
        auto result = this->working_buffer.begin() + offset;
        set(result + sizeof(ChecksumFrame<Backend>), block_size);
        current_frame = -1;
    }

    off_t getPositionInFile() override { return (current_frame == -1ull) ? 0 : current_frame * frame_size + offset(); }

    size_t readBig(char * buffer, size_t size) override
    {
        const auto expected = size;
        auto & frame = reinterpret_cast<ChecksumFrame<Backend> &>(
            *(this->working_buffer.begin() - sizeof(ChecksumFrame<Backend>))); // align should not fail

        auto readHeader = [&]() -> bool {
            auto header_length = expectRead(working_buffer.begin() - sizeof(ChecksumFrame<Backend>), sizeof(ChecksumFrame<Backend>));
            if (header_length == 0)
                return false;
            if (unlikely(header_length != sizeof(ChecksumFrame<Backend>)))
            {
                throw TiFlashException(fmt::format("readBig expects to read a new header, but only {}/{} bytes returned",
                                           header_length,
                                           sizeof(ChecksumFrame<Backend>)),
                    Errors::Checksum::IOFailure);
            }
            return true;
        };

        auto readBody = [&]() {
            auto body_length = expectRead(buffer, frame.bytes);
            if (unlikely(body_length != frame.bytes))
            {
                throw TiFlashException(
                    fmt::format("readBig expects to read the body, but only {}/{} bytes returned", body_length, frame.bytes),
                    Errors::Checksum::IOFailure);
            }
        };

        // firstly, if we have read some bytes
        // we need to flush them to the destination
        if (working_buffer.end() - position() != 0)
        {
            auto amount = std::min(size, static_cast<size_t>(working_buffer.end() - position()));
            std::memcpy(buffer, position(), amount);
            size -= amount;
            position() += amount;
            buffer += amount;
        }

        // now, we are at the beginning of the next frame
        while (size >= frame_size)
        {
            // read the header to our own memory area
            // if readHeader returns false, then we are at the end of file
            if (!readHeader())
            {
                return expected - size;
            }

            // read the body
            readBody();

            // check body
            if (!skip_checksum)
            {
                auto digest = Backend{};
                digest.update(buffer, frame.bytes);
                if (unlikely(frame.checksum != digest.checksum()))
                {
                    throw TiFlashException("checksum mismatch for " + in->getFileName(), Errors::Checksum::DataCorruption);
                }
            }

            // update statistics
            current_frame++;
            size -= frame.bytes;
            buffer += frame.bytes;
            position() = working_buffer.end();
        }

        // Finally, there may be still some bytes left.
        if (size > 0)
        {
            return (expected - size) + read(buffer, size);
        }
        else
        {
            return expected;
        }
    }

private:
    size_t current_frame;
    const size_t frame_size;
    const bool skip_checksum;
    RandomAccessFilePtr in;
    size_t expectRead(Position pos, size_t size)
    {
        size_t expected = size;
        while (expected != 0)
        {
            ProfileEvents::increment(ProfileEvents::ChecksumBufferRead);
            ssize_t count;
            {
                count = in->read(pos, expected);
            }
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
                    throw TiFlashException(fmt::format("cannot load checksum framed data from {} (errno = {})", in->getFileName(), errno),
                        Errors::Checksum::IOFailure);
                }
            }
            expected -= count;
            pos += count;
        }
        ProfileEvents::increment(ProfileEvents::ChecksumBufferReadBytes, size - expected);
        return size - expected;
    }

    void checkBody()
    {
        auto & frame = reinterpret_cast<ChecksumFrame<Backend> &>(
            *(this->working_buffer.begin() - sizeof(ChecksumFrame<Backend>))); // align should not fail

        // examine checksum
        if (!skip_checksum)
        {
            auto digest = Backend{};
            digest.update(frame.data, frame.bytes);
            if (unlikely(frame.checksum != digest.checksum()))
            {
                throw TiFlashException("checksum mismatch for " + in->getFileName(), Errors::Checksum::DataCorruption);
            }
        }

        // shift position, because the last frame may be of less length
        working_buffer.resize(frame.bytes);
    }

    bool nextImpl() override
    {
        auto & frame = reinterpret_cast<ChecksumFrame<Backend> &>(
            *(this->working_buffer.begin() - sizeof(ChecksumFrame<Backend>))); // align should not fail

        // read header and body
        auto length = expectRead(working_buffer.begin() - sizeof(ChecksumFrame<Backend>), sizeof(ChecksumFrame<Backend>) + frame_size);
        if (length == 0)
            return false; // EOF
        if (unlikely(length != sizeof(ChecksumFrame<Backend>) + frame.bytes))
        {
            throw TiFlashException(fmt::format("frame length (header = {}, body = {}, read = {}) mismatch for {}",
                                       sizeof(ChecksumFrame<Backend>),
                                       frame.bytes,
                                       length,
                                       in->getFileName()),
                Errors::Checksum::DataCorruption);
        }

        // body checksum examination
        checkBody();

        // update statistics
        current_frame++;
        return true;
    }

    off_t doSeek(off_t offset, int whence) override
    {
        ProfileEvents::increment(ProfileEvents::Seek);
        ProfileEvents::increment(ProfileEvents::ChecksumBufferSeek);

        auto & frame = reinterpret_cast<ChecksumFrame<Backend> &>(
            *(this->working_buffer.begin() - sizeof(ChecksumFrame<Backend>))); // align should not fail

        if (whence == SEEK_CUR)
        {
            offset = getPositionInFile() + offset;
        }
        else if (whence != SEEK_SET)
        {
            throw TiFlashException("FramedChecksumReadBuffer::seek expects SEEK_SET or SEEK_CUR as whence", Errors::Checksum::Internal);
        }
        auto target_frame = offset / frame_size;
        auto target_offset = offset % frame_size;

        if (target_frame == current_frame)
        {
            pos = working_buffer.begin() + target_offset;
            return offset;
        }
        else
        {
            // read the header and the body
            auto header_offset = target_frame * (sizeof(ChecksumFrame<Backend>) + frame_size);
            auto result = in->seek(static_cast<off_t>(header_offset), SEEK_SET);
            if (result == -1)
            {
                throw TiFlashException("checksum framed file " + in->getFileName() + " is not seekable", Errors::Checksum::IOFailure);
            }
            auto length = expectRead(working_buffer.begin() - sizeof(ChecksumFrame<Backend>), sizeof(ChecksumFrame<Backend>) + frame_size);
            if (length == 0)
            {
                current_frame = target_frame;
                pos = working_buffer.begin();
                working_buffer.resize(0);
                return offset; // EOF
            }
            if (unlikely(length != sizeof(ChecksumFrame<Backend>) + frame.bytes))
            {
                throw TiFlashException(fmt::format("frame length (header = {}, body = {}, read = {}) mismatch for {}",
                                           sizeof(ChecksumFrame<Backend>),
                                           frame.bytes,
                                           length,
                                           in->getFileName()),
                    Errors::Checksum::DataCorruption);
            }

            // body checksum examination
            checkBody();

            // update statistics
            current_frame = target_frame;
            pos = working_buffer.begin() + target_offset;
        }

        return offset;
    }
};

} // namespace DB
