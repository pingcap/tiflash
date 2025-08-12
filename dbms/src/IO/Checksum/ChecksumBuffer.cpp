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

#include <IO/Checksum/ChecksumBuffer.h>

namespace DB
{
using namespace DB::Digest;

template class FramedChecksumReadBuffer<None>;
template class FramedChecksumReadBuffer<CRC32>;
template class FramedChecksumReadBuffer<CRC64>;
template class FramedChecksumReadBuffer<City128>;
template class FramedChecksumReadBuffer<XXH3>;

template class FramedChecksumWriteBuffer<None>;
template class FramedChecksumWriteBuffer<CRC32>;
template class FramedChecksumWriteBuffer<CRC64>;
template class FramedChecksumWriteBuffer<City128>;
template class FramedChecksumWriteBuffer<XXH3>;

template <typename Backend>
off_t FramedChecksumReadBuffer<Backend>::doSeek(off_t offset, int whence)
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

} // namespace DB
