// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once
#ifndef TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE
#define TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE DBMS_DEFAULT_BUFFER_SIZE
#endif // TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE

#include <Common/Checksum.h>
#include <Common/TiFlashException.h>
#include <IO/BaseFile/RandomAccessFile.h>
#include <IO/BaseFile/WritableFile.h>
#include <IO/Buffer/ReadBufferFromFileDescriptor.h>
#include <IO/Buffer/WriteBufferFromFileDescriptor.h>
#include <fmt/format.h>

namespace ProfileEvents
{
extern const Event FileFSync;
extern const Event WriteBufferFromFileDescriptorWrite;
extern const Event WriteBufferFromFileDescriptorWriteBytes;
extern const Event ReadBufferFromFileDescriptorRead;
extern const Event ReadBufferFromFileDescriptorReadBytes;
extern const Event ReadBufferFromFileDescriptorReadFailed;
extern const Event Seek;
} // namespace ProfileEvents
namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_FSYNC;
} // namespace ErrorCodes

/**
 * A frame consists of a header and a body that conforms the following structure:
 *
 * \code
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
 * \endcode
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
 * To keep `PositionInFile` information and make sure the whole file is seekable by offset, one should
 * never invoke `sync/next` by hand unless one knows that it is at the end of frame.
 */


template <typename Backend>
class FramedChecksumWriteBuffer : public WriteBufferFromFileDescriptor
{
private:
    WritableFilePtr out;
    size_t materialized_bytes = 0;
    size_t frame_count = 0;
    const size_t frame_size;
#ifndef NDEBUG
    bool has_incomplete_frame = false;
#endif
    void nextImpl() override
    {
#ifndef NDEBUG
        if (offset() != this->working_buffer.size())
        {
            has_incomplete_frame = true;
        }
#endif
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
            ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWrite);

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
                    throw TiFlashException(
                        fmt::format("cannot flush checksum framed data to {} (errno = {})", out->getFileName(), errno),
                        Errors::Checksum::IOFailure);
                }
            }
            iter += count;
            materialized_bytes += count;
            expected -= count;
        }

        ProfileEvents::increment(
            ProfileEvents::WriteBufferFromFileDescriptorWriteBytes,
            len + sizeof(ChecksumFrame<Backend>));
        frame_count++;
    }

    off_t doSeek(off_t, int) override { throw Exception("framed file is not seekable in writing mode"); }

    // For checksum buffer, this is the **faked** file size without checksum header.
    // Statistics will be inaccurate after `sync/next` operation in the middle of a frame because it will
    // generate a frame without a full length.
    off_t getPositionInFile() override
    {
#ifndef NDEBUG
        assert(has_incomplete_frame == false);
#endif
        return frame_count * frame_size + offset();
    }

    // For checksum buffer, this is the real bytes to be materialized to disk.
    // We normally have `materialized bytes != position in file` in the sense that,
    // materialized bytes are referring to the real files on disk whereas position
    // in file are to make the underlying checksum implementation opaque to above layers
    // so that above buffers can do seek/read without knowing the existence of frame headers
    off_t getMaterializedBytes() override
    {
        return materialized_bytes + ((offset() != 0) ? (sizeof(ChecksumFrame<Backend>) + offset()) : 0);
    }

    void sync() override
    {
        next();

        int res = out->fsync();
        if (-1 == res)
            throwFromErrno("Cannot fsync " + getFileName(), ErrorCodes::CANNOT_FSYNC);
    }

    void close() override { out->close(); }

public:
    explicit FramedChecksumWriteBuffer(WritableFilePtr out_, size_t block_size_ = TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE)
        : WriteBufferFromFileDescriptor(
            out_->getFd(),
            sizeof(ChecksumFrame<Backend>) + block_size_ + 512,
            nullptr,
            alignof(ChecksumFrame<Backend>))
        , out(std::move(out_))
        , frame_size(block_size_)
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
        RandomAccessFilePtr in_,
        size_t block_size = TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE,
        bool skip_checksum_ = false)
        : ReadBufferFromFileDescriptor(
            in_->getFd(),
            sizeof(ChecksumFrame<Backend>) + block_size + 512,
            nullptr,
            alignof(ChecksumFrame<Backend>))
        , frame_size(block_size)
        , skip_checksum(skip_checksum_)
        , in(std::move(in_))
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

        auto read_header = [&]() -> bool {
            auto header_length
                = expectRead(working_buffer.begin() - sizeof(ChecksumFrame<Backend>), sizeof(ChecksumFrame<Backend>));
            if (header_length == 0)
                return false;
            if (unlikely(header_length != sizeof(ChecksumFrame<Backend>)))
            {
                throw TiFlashException(
                    fmt::format(
                        "readBig expects to read a new header, but only {}/{} bytes returned",
                        header_length,
                        sizeof(ChecksumFrame<Backend>)),
                    Errors::Checksum::IOFailure);
            }
            return true;
        };

        auto read_body = [&]() {
            auto body_length = expectRead(buffer, frame.bytes);
            if (unlikely(body_length != frame.bytes))
            {
                throw TiFlashException(
                    fmt::format(
                        "readBig expects to read the body, but only {}/{} bytes returned",
                        body_length,
                        frame.bytes),
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
            // if read_header returns false, then we are at the end of file
            if (!read_header())
            {
                return expected - size;
            }

            // read the body
            read_body();

            // check body
            if (!skip_checksum)
            {
                auto digest = Backend{};
                digest.update(buffer, frame.bytes);
                if (unlikely(frame.checksum != digest.checksum()))
                {
                    throw TiFlashException(
                        "checksum mismatch for " + in->getFileName(),
                        Errors::Checksum::DataCorruption);
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
            ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorRead);
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
                ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadFailed);
                if (errno == EINTR)
                    continue;
                else
                {
                    throw TiFlashException(
                        fmt::format("cannot load checksum framed data from {} (errno = {})", in->getFileName(), errno),
                        Errors::Checksum::IOFailure);
                }
            }
            expected -= count;
            pos += count;
        }
        ProfileEvents::increment(ProfileEvents::ReadBufferFromFileDescriptorReadBytes, size - expected);
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
        auto length = expectRead(
            working_buffer.begin() - sizeof(ChecksumFrame<Backend>),
            sizeof(ChecksumFrame<Backend>) + frame_size);
        if (length == 0)
            return false; // EOF
        if (unlikely(length != sizeof(ChecksumFrame<Backend>) + frame.bytes))
        {
            throw TiFlashException(
                fmt::format(
                    "frame length (header = {}, body = {}, read = {}) mismatch for {}",
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
        auto & frame = reinterpret_cast<ChecksumFrame<Backend> &>(
            *(this->working_buffer.begin() - sizeof(ChecksumFrame<Backend>))); // align should not fail

        if (whence == SEEK_CUR)
        {
            offset = getPositionInFile() + offset;
        }
        else if (whence != SEEK_SET)
        {
            throw TiFlashException(
                "FramedChecksumReadBuffer::seek expects SEEK_SET or SEEK_CUR as whence",
                Errors::Checksum::Internal);
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
                throw TiFlashException(
                    "checksum framed file " + in->getFileName() + " is not seekable",
                    Errors::Checksum::IOFailure);
            }
            auto length = expectRead(
                working_buffer.begin() - sizeof(ChecksumFrame<Backend>),
                sizeof(ChecksumFrame<Backend>) + frame_size);
            if (length == 0)
            {
                current_frame = target_frame;
                pos = working_buffer.begin();
                working_buffer.resize(0);
                return offset; // EOF
            }
            if (unlikely(length != sizeof(ChecksumFrame<Backend>) + frame.bytes))
            {
                throw TiFlashException(
                    fmt::format(
                        "frame length (header = {}, body = {}, read = {}) mismatch for {}",
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
