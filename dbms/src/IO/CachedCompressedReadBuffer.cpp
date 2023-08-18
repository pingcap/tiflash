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

#include <IO/CachedCompressedReadBuffer.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
extern const int SEEK_POSITION_OUT_OF_BOUND;
}


template <bool has_checksum>
void CachedCompressedReadBuffer<has_checksum>::initInput()
{
    if (!file_in)
    {
        file_in = createReadBufferFromFileBase(path, estimated_size, aio_threshold, buf_size);
        this->compressed_in = &*file_in;

        if (profile_callback)
            file_in->setProfileCallback(profile_callback, clock_type);
    }
}

template <bool has_checksum>
bool CachedCompressedReadBuffer<has_checksum>::nextImpl()
{
    /// Let's check for the presence of a decompressed block in the cache, grab the ownership of this block, if it exists.

    UInt128 key = cache->hash(path, file_pos);
    owned_cell = cache->get(key);

    if (!owned_cell)
    {
        /// If not, read it from the file.
        initInput();
        file_in->seek(file_pos);

        owned_cell = std::make_shared<UncompressedCacheCell>();

        size_t size_decompressed;
        size_t size_compressed_without_checksum;
        owned_cell->compressed_size = this->readCompressedData(size_decompressed, size_compressed_without_checksum);

        if (owned_cell->compressed_size)
        {
            owned_cell->data.resize(size_decompressed);
            this->decompress(owned_cell->data.m_data, size_decompressed, size_compressed_without_checksum);

            /// Put data into cache.
            cache->set(key, owned_cell);
        }
    }

    if (owned_cell->data.m_size == 0)
    {
        owned_cell = nullptr;
        return false;
    }

    working_buffer = Buffer(owned_cell->data.m_data, owned_cell->data.m_data + owned_cell->data.m_size);

    file_pos += owned_cell->compressed_size;

    return true;
}

template <bool has_checksum>
CachedCompressedReadBuffer<has_checksum>::CachedCompressedReadBuffer(
    const std::string & path_,
    UncompressedCache * cache_,
    size_t estimated_size_,
    size_t aio_threshold_,
    size_t buf_size_)
    : ReadBuffer(nullptr, 0)
    , path(path_)
    , cache(cache_)
    , buf_size(buf_size_)
    , estimated_size(estimated_size_)
    , aio_threshold(aio_threshold_)
    , file_pos(0)
{}

template <bool has_checksum>
void CachedCompressedReadBuffer<has_checksum>::seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block)
{
    if (owned_cell && offset_in_compressed_file == file_pos - owned_cell->compressed_size
        && offset_in_decompressed_block <= working_buffer.size())
    {
        bytes += offset();
        pos = working_buffer.begin() + offset_in_decompressed_block;
        bytes -= offset();
    }
    else
    {
        file_pos = offset_in_compressed_file;

        bytes += offset();
        nextImpl();

        if (offset_in_decompressed_block > working_buffer.size())
            throw Exception("Seek position is beyond the decompressed block"
                            " (pos: "
                                + toString(offset_in_decompressed_block) + ", block size: " + toString(working_buffer.size()) + ")",
                            ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

        pos = working_buffer.begin() + offset_in_decompressed_block;
        bytes -= offset();
    }
}

template class CachedCompressedReadBuffer<true>;
template class CachedCompressedReadBuffer<false>;

} // namespace DB
