// Copyright 2024 PingCAP, Inc.
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


#include <Common/RandomData.h>
#include <DataTypes/DataTypeString.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/Compression/CompressedReadBuffer.h>
#include <IO/Compression/CompressedWriteBuffer.h>
#include <IO/Compression/CompressionSettings.h>
#include <IO/Encryption/MockKeyManager.h>
#include <IO/FileProvider/ChecksumWriteBufferBuilder.h>
#include <Poco/UUIDGenerator.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <benchmark/benchmark.h>

#include <random>
namespace DB::bench
{

String getStreamName(const String & column_name, const IDataType::SubstreamPath & substream_path)
{
    return IDataType::getFileNameForStream(column_name, substream_path);
}

ColumnPtr createColumnString(size_t str_size, size_t count)
{
    std::random_device rand_dev;
    std::mt19937_64 rand_gen(rand_dev());
    std::uniform_int_distribution<size_t> rand_size(str_size * 0.8, str_size * 1.2);
    std::vector<String> v(count);
    for (auto & s : v)
        s = DB::random::randomString(rand_size(rand_gen));

    return DB::tests::createColumn<String>(v, "", 0).column;
}

using WriteBufferPair = std::pair<std::unique_ptr<WriteBuffer>, std::unique_ptr<WriteBufferFromOwnString>>;
WriteBufferPair createWriteBuffer(const String & stream_name, CompressionMethod method)
{
    auto write_buffer = std::make_unique<WriteBufferFromOwnString>(100 * 1024 * 1024);
    std::unique_ptr<WriteBuffer> compressed_buf;
    if (method != CompressionMethod::NONE)
    {
        CompressionSetting setting{method};
        setting.data_type = stream_name.ends_with(".size") ? CompressionDataType::Int64 : CompressionDataType::String;
        compressed_buf = CompressedWriteBuffer<>::build(*write_buffer, CompressionSettings{setting}, false);
    }
    return {std::move(compressed_buf), std::move(write_buffer)};
}

using ReadBufferPair = std::pair<std::unique_ptr<ReadBuffer>, std::unique_ptr<ReadBuffer>>;
ReadBufferPair createReadBuffer(const WriteBufferFromOwnString & write_buffer, bool enable_compression)
{
    auto read_buffer = std::make_unique<ReadBufferFromString>(write_buffer.stringRef().toStringView());
    std::unique_ptr<ReadBuffer> compressed_buf;
    if (enable_compression)
        compressed_buf = std::make_unique<CompressedReadBuffer<false>>(*read_buffer);
    return {std::move(compressed_buf), std::move(read_buffer)};
}

auto initWriteStream(IDataType & type, CompressionMethod method)
{
    std::unordered_map<String, WriteBufferPair> write_streams;
    auto create_write_stream = [&](const IDataType::SubstreamPath & substream_path) {
        const auto stream_name = getStreamName("bench", substream_path);
        write_streams.emplace(stream_name, createWriteBuffer(stream_name, method));
    };
    type.enumerateStreams(create_write_stream, {});
    return write_streams;
}

constexpr size_t str_count = 65535;

template <typename... Args>
void serialize(benchmark::State & state, Args &&... args)
{
    auto [version, str_size, method] = std::make_tuple(std::move(args)...);
    auto str_col = createColumnString(str_size, str_count);
    DataTypeString t(version);
    IDataType & type = t;
    auto write_streams = initWriteStream(type, method);
    auto get_write_stream = [&](const IDataType::SubstreamPath & substream_path) -> WriteBuffer * {
        const auto stream_name = getStreamName("bench", substream_path);
        auto & [compress_buf, write_buffer] = write_streams.at(stream_name);
        write_buffer->restart(); // Reset to avoid write buffer overflow.
        if (compress_buf)
            return compress_buf.get();
        return write_buffer.get();
    };
    auto flush_stream = [&](const IDataType::SubstreamPath & substream_path) {
        const auto stream_name = getStreamName("bench", substream_path);
        auto & [compress_buf, write_buffer] = write_streams.at(stream_name);
        if (compress_buf)
            compress_buf->next();
    };
    for (auto _ : state)
    {
        type.serializeBinaryBulkWithMultipleStreams(*str_col, get_write_stream, 0, str_col->size(), true, {});
        type.enumerateStreams(flush_stream, {});
    }
}

template <typename... Args>
void deserialize(benchmark::State & state, Args &&... args)
{
    auto [version, str_size, method] = std::make_tuple(std::move(args)...);
    auto str_col = createColumnString(str_size, str_count);
    DataTypeString t(version);
    IDataType & type = t;
    auto write_streams = initWriteStream(type, method);
    auto get_write_stream = [&](const IDataType::SubstreamPath & substream_path) -> WriteBuffer * {
        const auto stream_name = getStreamName("bench", substream_path);
        auto & [compress_buf, write_buffer] = write_streams.at(stream_name);
        if (compress_buf)
            return compress_buf.get();
        return write_buffer.get();
    };
    auto flush_stream = [&](const IDataType::SubstreamPath & substream_path) {
        const auto stream_name = getStreamName("bench", substream_path);
        auto & [compress_buf, write_buffer] = write_streams.at(stream_name);
        if (compress_buf)
            compress_buf->next();
    };
    type.serializeBinaryBulkWithMultipleStreams(*str_col, get_write_stream, 0, str_col->size(), true, {});
    type.enumerateStreams(flush_stream, {});

    std::unordered_map<String, ReadBufferPair> read_streams;
    auto get_read_stream = [&](const IDataType::SubstreamPath & substream_path) {
        const auto stream_name = getStreamName("bench", substream_path);
        auto & [compress_buf, write_buffer] = write_streams.at(stream_name);
        read_streams[stream_name] = createReadBuffer(*write_buffer, compress_buf != nullptr);
        auto & [compressed_read_buffer, read_buffer] = read_streams[stream_name];
        if (compressed_read_buffer)
            return compressed_read_buffer.get();
        return read_buffer.get();
    };
    for (auto _ : state)
    {
        auto col = type.createColumn();
        type.deserializeBinaryBulkWithMultipleStreams(*col, get_read_stream, str_count, str_size, true, {});
        benchmark::DoNotOptimize(col);
    }
}

BENCHMARK_CAPTURE(serialize, v0_size1_none, 0, 1, CompressionMethod::NONE);
BENCHMARK_CAPTURE(serialize, v0_size2_none, 0, 2, CompressionMethod::NONE);
BENCHMARK_CAPTURE(serialize, v0_size4_none, 0, 4, CompressionMethod::NONE);
BENCHMARK_CAPTURE(serialize, v0_size8_none, 0, 8, CompressionMethod::NONE);
BENCHMARK_CAPTURE(serialize, v0_size16_none, 0, 16, CompressionMethod::NONE);
BENCHMARK_CAPTURE(serialize, v0_size32_none, 0, 32, CompressionMethod::NONE);
BENCHMARK_CAPTURE(serialize, v0_size64_none, 0, 64, CompressionMethod::NONE);
BENCHMARK_CAPTURE(serialize, v0_size128_none, 0, 128, CompressionMethod::NONE);
BENCHMARK_CAPTURE(serialize, v0_size256_none, 0, 256, CompressionMethod::NONE);
BENCHMARK_CAPTURE(serialize, v0_size512_none, 0, 512, CompressionMethod::NONE);
BENCHMARK_CAPTURE(serialize, v0_size1024_none, 0, 1024, CompressionMethod::NONE);

BENCHMARK_CAPTURE(serialize, v1_size1_none, 1, 1, CompressionMethod::NONE);
BENCHMARK_CAPTURE(serialize, v1_size2_none, 1, 2, CompressionMethod::NONE);
BENCHMARK_CAPTURE(serialize, v1_size4_none, 1, 4, CompressionMethod::NONE);
BENCHMARK_CAPTURE(serialize, v1_size8_none, 1, 8, CompressionMethod::NONE);
BENCHMARK_CAPTURE(serialize, v1_size16_none, 1, 16, CompressionMethod::NONE);
BENCHMARK_CAPTURE(serialize, v1_size32_none, 1, 32, CompressionMethod::NONE);
BENCHMARK_CAPTURE(serialize, v1_size64_none, 1, 64, CompressionMethod::NONE);
BENCHMARK_CAPTURE(serialize, v1_size128_none, 1, 128, CompressionMethod::NONE);
BENCHMARK_CAPTURE(serialize, v1_size256_none, 1, 256, CompressionMethod::NONE);
BENCHMARK_CAPTURE(serialize, v1_size512_none, 1, 512, CompressionMethod::NONE);
BENCHMARK_CAPTURE(serialize, v1_size1024_none, 1, 1024, CompressionMethod::NONE);

BENCHMARK_CAPTURE(deserialize, v0_size1_none, 0, 1, CompressionMethod::NONE);
BENCHMARK_CAPTURE(deserialize, v0_size2_none, 0, 2, CompressionMethod::NONE);
BENCHMARK_CAPTURE(deserialize, v0_size4_none, 0, 4, CompressionMethod::NONE);
BENCHMARK_CAPTURE(deserialize, v0_size8_none, 0, 8, CompressionMethod::NONE);
BENCHMARK_CAPTURE(deserialize, v0_size16_none, 0, 16, CompressionMethod::NONE);
BENCHMARK_CAPTURE(deserialize, v0_size32_none, 0, 32, CompressionMethod::NONE);
BENCHMARK_CAPTURE(deserialize, v0_size64_none, 0, 64, CompressionMethod::NONE);
BENCHMARK_CAPTURE(deserialize, v0_size128_none, 0, 128, CompressionMethod::NONE);
BENCHMARK_CAPTURE(deserialize, v0_size256_none, 0, 256, CompressionMethod::NONE);
BENCHMARK_CAPTURE(deserialize, v0_size512_none, 0, 512, CompressionMethod::NONE);
BENCHMARK_CAPTURE(deserialize, v0_size1024_none, 0, 1024, CompressionMethod::NONE);

BENCHMARK_CAPTURE(deserialize, v1_size1_none, 1, 1, CompressionMethod::NONE);
BENCHMARK_CAPTURE(deserialize, v1_size2_none, 1, 2, CompressionMethod::NONE);
BENCHMARK_CAPTURE(deserialize, v1_size4_none, 1, 4, CompressionMethod::NONE);
BENCHMARK_CAPTURE(deserialize, v1_size8_none, 1, 8, CompressionMethod::NONE);
BENCHMARK_CAPTURE(deserialize, v1_size16_none, 1, 16, CompressionMethod::NONE);
BENCHMARK_CAPTURE(deserialize, v1_size32_none, 1, 32, CompressionMethod::NONE);
BENCHMARK_CAPTURE(deserialize, v1_size64_none, 1, 64, CompressionMethod::NONE);
BENCHMARK_CAPTURE(deserialize, v1_size128_none, 1, 128, CompressionMethod::NONE);
BENCHMARK_CAPTURE(deserialize, v1_size256_none, 1, 256, CompressionMethod::NONE);
BENCHMARK_CAPTURE(deserialize, v1_size512_none, 1, 512, CompressionMethod::NONE);
BENCHMARK_CAPTURE(deserialize, v1_size1024_none, 1, 1024, CompressionMethod::NONE);

BENCHMARK_CAPTURE(serialize, v0_size1_lz4, 0, 1, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(serialize, v0_size2_lz4, 0, 2, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(serialize, v0_size4_lz4, 0, 4, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(serialize, v0_size8_lz4, 0, 8, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(serialize, v0_size16_lz4, 0, 16, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(serialize, v0_size32_lz4, 0, 32, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(serialize, v0_size64_lz4, 0, 64, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(serialize, v0_size128_lz4, 0, 128, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(serialize, v0_size256_lz4, 0, 256, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(serialize, v0_size512_lz4, 0, 512, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(serialize, v0_size1024_lz4, 0, 1024, CompressionMethod::LZ4);

BENCHMARK_CAPTURE(serialize, v1_size1_lz4, 1, 1, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(serialize, v1_size2_lz4, 1, 2, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(serialize, v1_size4_lz4, 1, 4, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(serialize, v1_size8_lz4, 1, 8, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(serialize, v1_size16_lz4, 1, 16, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(serialize, v1_size32_lz4, 1, 32, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(serialize, v1_size64_lz4, 1, 64, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(serialize, v1_size128_lz4, 1, 128, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(serialize, v1_size256_lz4, 1, 256, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(serialize, v1_size512_lz4, 1, 512, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(serialize, v1_size1024_lz4, 1, 1024, CompressionMethod::LZ4);

BENCHMARK_CAPTURE(deserialize, v0_size1_lz4, 0, 1, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(deserialize, v0_size2_lz4, 0, 2, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(deserialize, v0_size4_lz4, 0, 4, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(deserialize, v0_size8_lz4, 0, 8, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(deserialize, v0_size16_lz4, 0, 16, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(deserialize, v0_size32_lz4, 0, 32, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(deserialize, v0_size64_lz4, 0, 64, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(deserialize, v0_size128_lz4, 0, 128, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(deserialize, v0_size256_lz4, 0, 256, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(deserialize, v0_size512_lz4, 0, 512, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(deserialize, v0_size1024_lz4, 0, 1024, CompressionMethod::LZ4);

BENCHMARK_CAPTURE(deserialize, v1_size1_lz4, 1, 1, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(deserialize, v1_size2_lz4, 1, 2, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(deserialize, v1_size4_lz4, 1, 4, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(deserialize, v1_size8_lz4, 1, 8, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(deserialize, v1_size16_lz4, 1, 16, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(deserialize, v1_size32_lz4, 1, 32, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(deserialize, v1_size64_lz4, 1, 64, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(deserialize, v1_size128_lz4, 1, 128, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(deserialize, v1_size256_lz4, 1, 256, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(deserialize, v1_size512_lz4, 1, 512, CompressionMethod::LZ4);
BENCHMARK_CAPTURE(deserialize, v1_size1024_lz4, 1, 1024, CompressionMethod::LZ4);

BENCHMARK_CAPTURE(serialize, v1_size1_lw, 1, 1, CompressionMethod::Lightweight);
BENCHMARK_CAPTURE(serialize, v1_size2_lw, 1, 2, CompressionMethod::Lightweight);
BENCHMARK_CAPTURE(serialize, v1_size4_lw, 1, 4, CompressionMethod::Lightweight);
BENCHMARK_CAPTURE(serialize, v1_size8_lw, 1, 8, CompressionMethod::Lightweight);
BENCHMARK_CAPTURE(serialize, v1_size16_lw, 1, 16, CompressionMethod::Lightweight);
BENCHMARK_CAPTURE(serialize, v1_size32_lw, 1, 32, CompressionMethod::Lightweight);
BENCHMARK_CAPTURE(serialize, v1_size64_lw, 1, 64, CompressionMethod::Lightweight);
BENCHMARK_CAPTURE(serialize, v1_size128_lw, 1, 128, CompressionMethod::Lightweight);
BENCHMARK_CAPTURE(serialize, v1_size256_lw, 1, 256, CompressionMethod::Lightweight);
BENCHMARK_CAPTURE(serialize, v1_size512_lw, 1, 512, CompressionMethod::Lightweight);
BENCHMARK_CAPTURE(serialize, v1_size1024_lw, 1, 1024, CompressionMethod::Lightweight);

BENCHMARK_CAPTURE(deserialize, v1_size1_lw, 1, 1, CompressionMethod::Lightweight);
BENCHMARK_CAPTURE(deserialize, v1_size2_lw, 1, 2, CompressionMethod::Lightweight);
BENCHMARK_CAPTURE(deserialize, v1_size4_lw, 1, 4, CompressionMethod::Lightweight);
BENCHMARK_CAPTURE(deserialize, v1_size8_lw, 1, 8, CompressionMethod::Lightweight);
BENCHMARK_CAPTURE(deserialize, v1_size16_lw, 1, 16, CompressionMethod::Lightweight);
BENCHMARK_CAPTURE(deserialize, v1_size32_lw, 1, 32, CompressionMethod::Lightweight);
BENCHMARK_CAPTURE(deserialize, v1_size64_lw, 1, 64, CompressionMethod::Lightweight);
BENCHMARK_CAPTURE(deserialize, v1_size128_lw, 1, 128, CompressionMethod::Lightweight);
BENCHMARK_CAPTURE(deserialize, v1_size256_lw, 1, 256, CompressionMethod::Lightweight);
BENCHMARK_CAPTURE(deserialize, v1_size512_lw, 1, 512, CompressionMethod::Lightweight);
BENCHMARK_CAPTURE(deserialize, v1_size1024_lw, 1, 1024, CompressionMethod::Lightweight);
} // namespace DB::bench
