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

#include <Common/config.h>
#include <IO/Compression/CompressedReadBuffer.h>

namespace DB
{
template <bool has_legacy_checksum>
bool CompressedReadBuffer<has_legacy_checksum>::nextImpl()
{
    size_t size_decompressed;
    size_t size_compressed_without_checksum;
    size_compressed = this->readCompressedData(size_decompressed, size_compressed_without_checksum);
    if (!size_compressed)
        return false;

    assert(size_decompressed > 0 && size_compressed_without_checksum > 0);
    memory.resize(size_decompressed);
    working_buffer = Buffer(&memory[0], &memory[size_decompressed]);

    this->decompress(working_buffer.begin(), size_decompressed, size_compressed_without_checksum);

    return true;
}

template <bool has_legacy_checksum>
size_t CompressedReadBuffer<has_legacy_checksum>::readBig(char * to, size_t n)
{
    size_t bytes_read = 0;

    /// If there are unread bytes in the buffer, then we copy necessary to `to`.
    if (pos < working_buffer.end())
        bytes_read += read(to, std::min(static_cast<size_t>(working_buffer.end() - pos), n));

    /// If you need to read more - we will, if possible, uncompress at once to `to`.
    while (bytes_read < n)
    {
        size_t size_decompressed;
        size_t size_compressed_without_checksum;

        if (!this->readCompressedData(size_decompressed, size_compressed_without_checksum))
            return bytes_read;

        /// If the decompressed block is placed entirely where it needs to be copied.
        if (size_decompressed <= n - bytes_read)
        {
            this->decompress(to + bytes_read, size_decompressed, size_compressed_without_checksum);
            bytes_read += size_decompressed;
            bytes += size_decompressed;
        }
        else
        {
            bytes += offset();
            assert(size_decompressed > 0 && size_compressed_without_checksum > 0);
            memory.resize(size_decompressed);
            working_buffer = Buffer(&memory[0], &memory[size_decompressed]);
            pos = working_buffer.begin();

            this->decompress(working_buffer.begin(), size_decompressed, size_compressed_without_checksum);

            bytes_read += read(to + bytes_read, n - bytes_read);
            break;
        }
    }

    return bytes_read;
}

template class CompressedReadBuffer<true>;
template class CompressedReadBuffer<false>;

} // namespace DB
