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

#include <Core/Types.h>
#include <IO/Compression/CompressedWriteBuffer.h>
#include <IO/Compression/CompressionCodecFactory.h>
#include <IO/Compression/CompressionInfo.h>
#include <city.h>

namespace DB
{

template <bool add_legacy_checksum>
void CompressedWriteBuffer<add_legacy_checksum>::nextImpl()
{
    if (!offset())
        return;

    const char * source = working_buffer.begin();
    const size_t source_size = offset();
    UInt32 compressed_reserve_size = codec->getCompressedReserveSize(source_size);
    compressed_buffer.resize(compressed_reserve_size);
    auto compressed_size = codec->compress(source, source_size, compressed_buffer.data());

    if constexpr (add_legacy_checksum)
    {
        CityHash_v1_0_2::uint128 checksum = CityHash_v1_0_2::CityHash128(compressed_buffer.data(), compressed_size);
        out.write(reinterpret_cast<const char *>(&checksum), sizeof(checksum));
    }

    out.write(compressed_buffer.data(), compressed_size);
}

template <bool add_legacy_checksum>
CompressedWriteBuffer<add_legacy_checksum>::CompressedWriteBuffer(
    WriteBuffer & out_,
    CompressionSettings compression_settings_,
    size_t buf_size)
    : BufferWithOwnMemory<WriteBuffer>(buf_size)
    , out(out_)
    , compression_settings(compression_settings_)
    , codec(CompressionCodecFactory::create(compression_settings))
{}

template <bool add_legacy_checksum>
CompressedWriteBuffer<add_legacy_checksum>::~CompressedWriteBuffer()
{
    try
    {
        next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

template class CompressedWriteBuffer<true>;
template class CompressedWriteBuffer<false>;

} // namespace DB
