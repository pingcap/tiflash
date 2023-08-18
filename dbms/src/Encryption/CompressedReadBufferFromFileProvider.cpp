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

#include <Encryption/CompressedReadBufferFromFileProvider.h>
#include <Encryption/createReadBufferFromFileBaseByFileProvider.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
extern const int SEEK_POSITION_OUT_OF_BOUND;
}

template <bool has_checksum>
bool CompressedReadBufferFromFileProvider<has_checksum>::nextImpl()
{
    size_t size_decompressed;
    size_t size_compressed_without_checksum;
    size_compressed = this->readCompressedData(size_decompressed, size_compressed_without_checksum);
    if (!size_compressed)
        return false;

    assert(size_decompressed > 0);
    memory.resize(size_decompressed);
    working_buffer = Buffer(&memory[0], &memory[size_decompressed]);

    this->decompress(working_buffer.begin(), size_decompressed, size_compressed_without_checksum);

    return true;
}

template <bool has_checksum>
CompressedReadBufferFromFileProvider<has_checksum>::CompressedReadBufferFromFileProvider(
    FileProviderPtr & file_provider,
    const std::string & path,
    const EncryptionPath & encryption_path,
    size_t estimated_size,
    size_t aio_threshold,
    const ReadLimiterPtr & read_limiter_,
    size_t buf_size)
    : CompressedSeekableReaderBuffer()
    , p_file_in(createReadBufferFromFileBaseByFileProvider(
          file_provider,
          path,
          encryption_path,
          estimated_size,
          aio_threshold,
          read_limiter_,
          buf_size))
    , file_in(*p_file_in)
{
    this->compressed_in = &file_in;
}

template <bool has_checksum>
CompressedReadBufferFromFileProvider<has_checksum>::CompressedReadBufferFromFileProvider(
    FileProviderPtr & file_provider,
    const std::string & path,
    const EncryptionPath & encryption_path,
    size_t estimated_size,
    const ReadLimiterPtr & read_limiter_,
    ChecksumAlgo checksum_algorithm,
    size_t checksum_frame_size)
    : CompressedSeekableReaderBuffer()
    , p_file_in(createReadBufferFromFileBaseByFileProvider(
          file_provider,
          path,
          encryption_path,
          estimated_size,
          read_limiter_,
          checksum_algorithm,
          checksum_frame_size))
    , file_in(*p_file_in)
{
    this->compressed_in = &file_in;
}

template <bool has_checksum>
CompressedReadBufferFromFileProvider<has_checksum>::CompressedReadBufferFromFileProvider(
    String && data,
    const String & file_name,
    size_t estimated_size,
    ChecksumAlgo checksum_algorithm,
    size_t checksum_frame_size)
    : CompressedSeekableReaderBuffer()
    , p_file_in(createReadBufferFromData(
          std::forward<String>(data),
          file_name,
          estimated_size,
          checksum_algorithm,
          checksum_frame_size))
    , file_in(*p_file_in)
{
    this->compressed_in = &file_in;
}

template <bool has_checksum>
void CompressedReadBufferFromFileProvider<has_checksum>::seek(
    size_t offset_in_compressed_file,
    size_t offset_in_decompressed_block)
{
    if (size_compressed && offset_in_compressed_file == file_in.getPositionInFile() - size_compressed
        && offset_in_decompressed_block <= working_buffer.size())
    {
        bytes += offset();
        pos = working_buffer.begin() + offset_in_decompressed_block;
        /// `bytes` can overflow and get negative, but in `count()` everything will overflow back and get right.
        bytes -= offset();
    }
    else
    {
        file_in.seek(offset_in_compressed_file);

        bytes += offset();
        nextImpl();

        if (offset_in_decompressed_block > working_buffer.size())
            throw Exception(
                "Seek position is beyond the decompressed block"
                " (pos: "
                    + toString(offset_in_decompressed_block) + ", block size: " + toString(working_buffer.size()) + ")",
                ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

        pos = working_buffer.begin() + offset_in_decompressed_block;
        bytes -= offset();
    }
}

template <bool has_checksum>
size_t CompressedReadBufferFromFileProvider<has_checksum>::readBig(char * to, size_t n)
{
    size_t bytes_read = 0;

    /// If there are unread bytes in the buffer, then we copy needed to `to`.
    if (pos < working_buffer.end())
        bytes_read += read(to, std::min(static_cast<size_t>(working_buffer.end() - pos), n));

    /// If you need to read more - we will, if possible, decompress at once to `to`.
    while (bytes_read < n)
    {
        size_t size_decompressed = 0;
        size_t size_compressed_without_checksum = 0;

        size_t new_size_compressed = this->readCompressedData(size_decompressed, size_compressed_without_checksum);
        size_compressed = 0; /// file_in no longer points to the end of the block in working_buffer.
        if (!new_size_compressed)
            return bytes_read;

        /// If the decompressed block fits entirely where it needs to be copied.
        if (size_decompressed <= n - bytes_read)
        {
            this->decompress(to + bytes_read, size_decompressed, size_compressed_without_checksum);
            bytes_read += size_decompressed;
            bytes += size_decompressed;
        }
        else
        {
            size_compressed = new_size_compressed;
            bytes += offset();
            assert(size_decompressed > 0);
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

template class CompressedReadBufferFromFileProvider<true>;
template class CompressedReadBufferFromFileProvider<false>;

} // namespace DB
