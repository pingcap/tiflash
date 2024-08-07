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

#include <Core/Defines.h>
#include <IO/BaseFile/PosixWritableFile.h>
#include <IO/Buffer/ReadBufferFromFile.h>
#include <IO/Buffer/WriteBufferFromWritableFile.h>
#include <IO/Compression/CompressedReadBuffer.h>
#include <IO/Compression/CompressedWriteBuffer.h>
#include <IO/Compression/CompressionFactory.h>
#include <IO/Compression/tests/CodecTestSequence.h>
#include <Poco/File.h>
#include <benchmark/benchmark.h>


namespace DB::bench
{

template <CompressionMethodByte method_byte, typename T, class... Args>
static void singleWrite(benchmark::State & state, Args &&... args)
{
    auto args_tuple = std::make_tuple(std::move(args)...);
    auto generator = std::get<0>(args_tuple);
    auto sequence = tests::generateSeq<T>(generator, "", 0, 8192);
    auto file_name = fmt::format("/tmp/tiflash_codec_bench_{}_{}", sequence.name, magic_enum::enum_name(method_byte));
    for (auto _ : state)
    {
        auto file = std::make_shared<DB::PosixWritableFile>(file_name, true, -1, 0755);
        auto write_buffer = std::make_shared<DB::WriteBufferFromWritableFile>(file);
        CompressionSetting setting(method_byte);
        setting.data_type = magic_enum::enum_cast<CompressionDataType>(sizeof(T)).value();
        CompressedWriteBuffer<> compressed(*write_buffer, CompressionSettings(setting));
        compressed.write(sequence.serialized_data.data(), sequence.serialized_data.size());
        compressed.next();
        write_buffer->next();
        write_buffer->sync();
        Poco::File(file_name).remove();
    }
}

template <CompressionMethodByte method_byte, typename T, class... Args>
static void singleRead(benchmark::State & state, Args &&... args)
{
    auto args_tuple = std::make_tuple(std::move(args)...);
    auto generator = std::get<0>(args_tuple);
    auto sequence = tests::generateSeq<T>(generator, "", 0, 8192);
    auto file_name = fmt::format("/tmp/tiflash_codec_bench_{}_{}", sequence.name, magic_enum::enum_name(method_byte));
    {
        auto file = std::make_shared<DB::PosixWritableFile>(file_name, true, -1, 0755);
        auto write_buffer = std::make_shared<DB::WriteBufferFromWritableFile>(file);
        CompressionSetting setting(method_byte);
        setting.data_type = magic_enum::enum_cast<CompressionDataType>(sizeof(T)).value();
        CompressedWriteBuffer<> compressed(*write_buffer, CompressionSettings(setting));
        compressed.write(sequence.serialized_data.data(), sequence.serialized_data.size());
        compressed.next();
        write_buffer->next();
        write_buffer->sync();
    }
    for (auto _ : state)
    {
        auto read_buffer = std::make_shared<ReadBufferFromFile>(file_name);
        CompressedReadBuffer<> compressed(*read_buffer);
        const size_t buffer_size = 32 * 1024; // 32KB
        while (!compressed.eof())
        {
            char buffer[buffer_size];
            compressed.readBig(buffer, buffer_size);
            benchmark::DoNotOptimize(buffer);
        }
    }
    Poco::File(file_name).remove();
}

#define BENCH_SINGLE_WRITE_METHOD_GENERATOR_TYPE(name, method, generator, T) \
    template <typename... Args>                                              \
    static void name(benchmark::State & state, Args &&... args)              \
    {                                                                        \
        singleWrite<method, T>(state, args...);                              \
    }                                                                        \
    BENCHMARK_CAPTURE(name, generator, generator);

#define BENCH_SINGLE_READ_METHOD_GENERATOR_TYPE(name, method, generator, T) \
    template <typename... Args>                                             \
    static void name(benchmark::State & state, Args &&... args)             \
    {                                                                       \
        singleRead<method, T>(state, args...);                              \
    }                                                                       \
    BENCHMARK_CAPTURE(name, generator, generator);

#define BENCH_SINGLE_WRITE_GENERATOR_TYPE(name, generator, type)                                      \
    BENCH_SINGLE_WRITE_METHOD_GENERATOR_TYPE(name##LZ4, CompressionMethodByte::LZ4, generator, type); \
    BENCH_SINGLE_WRITE_METHOD_GENERATOR_TYPE(name##Lightweight, CompressionMethodByte::Lightweight, generator, type);

#define BENCH_SINGLE_READ_GENERATOR_TYPE(name, generator, type)                                      \
    BENCH_SINGLE_READ_METHOD_GENERATOR_TYPE(name##LZ4, CompressionMethodByte::LZ4, generator, type); \
    BENCH_SINGLE_READ_METHOD_GENERATOR_TYPE(name##Lightweight, CompressionMethodByte::Lightweight, generator, type);

#define BENCH_SINGLE_WRITE_GENERATOR(name, generator)                   \
    BENCH_SINGLE_WRITE_GENERATOR_TYPE(name##UInt8, generator, UInt8);   \
    BENCH_SINGLE_WRITE_GENERATOR_TYPE(name##UInt16, generator, UInt16); \
    BENCH_SINGLE_WRITE_GENERATOR_TYPE(name##UInt32, generator, UInt32); \
    BENCH_SINGLE_WRITE_GENERATOR_TYPE(name##UInt64, generator, UInt64);

#define BENCH_SINGLE_READ_GENERATOR(name, generator)                   \
    BENCH_SINGLE_READ_GENERATOR_TYPE(name##UInt8, generator, UInt8);   \
    BENCH_SINGLE_READ_GENERATOR_TYPE(name##UInt16, generator, UInt16); \
    BENCH_SINGLE_READ_GENERATOR_TYPE(name##UInt32, generator, UInt32); \
    BENCH_SINGLE_READ_GENERATOR_TYPE(name##UInt64, generator, UInt64);

#define BENCH_SINGLE_WRITE(name)                                                                                 \
    BENCH_SINGLE_WRITE_GENERATOR(name##SameValue, tests::SameValueGenerator(128))                                \
    BENCH_SINGLE_WRITE_GENERATOR(name##Sequential, tests::SequentialGenerator(2))                                \
    BENCH_SINGLE_WRITE_GENERATOR(name##SequentialReverse, tests::SequentialGenerator(-2))                        \
    BENCH_SINGLE_WRITE_GENERATOR(name##Monotonic, tests::MonotonicGenerator())                                   \
    BENCH_SINGLE_WRITE_GENERATOR(name##MonotonicReverse, tests::MonotonicGenerator(-1))                          \
    BENCH_SINGLE_WRITE_GENERATOR(name##MinMax, tests::MinMaxGenerator())                                         \
    BENCH_SINGLE_WRITE_GENERATOR_TYPE(name##RandomUInt8, tests::RandomGenerator<UInt8>(0), UInt8)                \
    BENCH_SINGLE_WRITE_GENERATOR_TYPE(name##RandomUInt16, tests::RandomGenerator<UInt16>(0), UInt16)             \
    BENCH_SINGLE_WRITE_GENERATOR_TYPE(name##RandomUInt32, tests::RandomGenerator<UInt32>(0), UInt32)             \
    BENCH_SINGLE_WRITE_GENERATOR_TYPE(name##RandomUInt64, tests::RandomGenerator<UInt64>(0), UInt64)             \
    BENCH_SINGLE_WRITE_GENERATOR_TYPE(name##SmallRandomUInt8, tests::RandomGenerator<UInt8>(0, 0, 16), UInt8)    \
    BENCH_SINGLE_WRITE_GENERATOR_TYPE(name##SmallRandomUInt16, tests::RandomGenerator<UInt16>(0, 0, 16), UInt16) \
    BENCH_SINGLE_WRITE_GENERATOR_TYPE(name##SmallRandomUInt32, tests::RandomGenerator<UInt32>(0, 0, 16), UInt32) \
    BENCH_SINGLE_WRITE_GENERATOR_TYPE(name##SmallRandomUInt64, tests::RandomGenerator<UInt64>(0, 0, 16), UInt64) \
    BENCH_SINGLE_WRITE_GENERATOR_TYPE(name##RepeatUInt8, tests::RepeatGenerator<UInt8>(0), UInt8)                \
    BENCH_SINGLE_WRITE_GENERATOR_TYPE(name##RepeatUInt16, tests::RepeatGenerator<UInt16>(0), UInt16)             \
    BENCH_SINGLE_WRITE_GENERATOR_TYPE(name##RepeatUInt32, tests::RepeatGenerator<UInt32>(0), UInt32)             \
    BENCH_SINGLE_WRITE_GENERATOR_TYPE(name##RepeatUInt64, tests::RepeatGenerator<UInt64>(0), UInt64)

#define BENCH_SINGLE_READ(name)                                                                                 \
    BENCH_SINGLE_READ_GENERATOR(name##SameValue, tests::SameValueGenerator(128))                                \
    BENCH_SINGLE_READ_GENERATOR(name##Sequential, tests::SequentialGenerator(2))                                \
    BENCH_SINGLE_READ_GENERATOR(name##SequentialReverse, tests::SequentialGenerator(-2))                        \
    BENCH_SINGLE_READ_GENERATOR(name##Monotonic, tests::MonotonicGenerator())                                   \
    BENCH_SINGLE_READ_GENERATOR(name##MonotonicReverse, tests::MonotonicGenerator(-1))                          \
    BENCH_SINGLE_READ_GENERATOR(name##MinMax, tests::MinMaxGenerator())                                         \
    BENCH_SINGLE_READ_GENERATOR_TYPE(name##RandomUInt8, tests::RandomGenerator<UInt8>(0), UInt8)                \
    BENCH_SINGLE_READ_GENERATOR_TYPE(name##RandomUInt16, tests::RandomGenerator<UInt16>(0), UInt16)             \
    BENCH_SINGLE_READ_GENERATOR_TYPE(name##RandomUInt32, tests::RandomGenerator<UInt32>(0), UInt32)             \
    BENCH_SINGLE_READ_GENERATOR_TYPE(name##RandomUInt64, tests::RandomGenerator<UInt64>(0), UInt64)             \
    BENCH_SINGLE_READ_GENERATOR_TYPE(name##SmallRandomUInt8, tests::RandomGenerator<UInt8>(0, 0, 16), UInt8)    \
    BENCH_SINGLE_READ_GENERATOR_TYPE(name##SmallRandomUInt16, tests::RandomGenerator<UInt16>(0, 0, 16), UInt16) \
    BENCH_SINGLE_READ_GENERATOR_TYPE(name##SmallRandomUInt32, tests::RandomGenerator<UInt32>(0, 0, 16), UInt32) \
    BENCH_SINGLE_READ_GENERATOR_TYPE(name##SmallRandomUInt64, tests::RandomGenerator<UInt64>(0, 0, 16), UInt64) \
    BENCH_SINGLE_READ_GENERATOR_TYPE(name##RepeatUInt8, tests::RepeatGenerator<UInt8>(0), UInt8)                \
    BENCH_SINGLE_READ_GENERATOR_TYPE(name##RepeatUInt16, tests::RepeatGenerator<UInt16>(0), UInt16)             \
    BENCH_SINGLE_READ_GENERATOR_TYPE(name##RepeatUInt32, tests::RepeatGenerator<UInt32>(0), UInt32)             \
    BENCH_SINGLE_READ_GENERATOR_TYPE(name##RepeatUInt64, tests::RepeatGenerator<UInt64>(0), UInt64)

BENCH_SINGLE_WRITE(CodecSingleWrite)
BENCH_SINGLE_READ(CodecSingleRead)

#define WRITE_SEQUENCE(generator)                                                           \
    {                                                                                       \
        auto sequence = tests::generateSeq<T>(generator, "", 0, 8192);                      \
        compressed.write(sequence.serialized_data.data(), sequence.serialized_data.size()); \
        compressed.next();                                                                  \
    }

template <CompressionMethodByte method_byte, typename T>
static void multipleWrite(benchmark::State & state)
{
    auto file_name = fmt::format("/tmp/tiflash_codec_bench_{}", magic_enum::enum_name(method_byte));
    for (auto _ : state)
    {
        auto file = std::make_shared<DB::PosixWritableFile>(file_name, true, -1, 0755);
        auto write_buffer = std::make_shared<DB::WriteBufferFromWritableFile>(file);
        CompressionSetting setting(method_byte);
        setting.data_type = magic_enum::enum_cast<CompressionDataType>(sizeof(T)).value();
        CompressedWriteBuffer<> compressed(*write_buffer, CompressionSettings(setting));

        WRITE_SEQUENCE(tests::SameValueGenerator(128)); // Constant
        WRITE_SEQUENCE(tests::SequentialGenerator(2)); // ConstantDelta
        WRITE_SEQUENCE(tests::SequentialGenerator(-2)); // ConstantDelta
        WRITE_SEQUENCE(tests::MonotonicGenerator()); // DeltaFOR
        WRITE_SEQUENCE(tests::MonotonicGenerator(-1)); // DeltaFOR
        WRITE_SEQUENCE(tests::MinMaxGenerator()); // DeltaFOR, (max - min = -1)
        WRITE_SEQUENCE(tests::RandomGenerator<T>(0)); // LZ4
        WRITE_SEQUENCE(tests::RandomGenerator<T>(0, 0, 100)); // FOR
        WRITE_SEQUENCE(tests::RepeatGenerator<T>(0)); // RLE

        write_buffer->next();
        write_buffer->sync();
        Poco::File(file_name).remove();
    }
}

template <CompressionMethodByte method_byte, typename T>
static void multipleRead(benchmark::State & state)
{
    auto file_name = fmt::format("/tmp/tiflash_codec_bench_{}", magic_enum::enum_name(method_byte));
    {
        auto file = std::make_shared<DB::PosixWritableFile>(file_name, true, -1, 0755);
        auto write_buffer = std::make_shared<DB::WriteBufferFromWritableFile>(file);
        CompressionSetting setting(method_byte);
        setting.data_type = magic_enum::enum_cast<CompressionDataType>(sizeof(T)).value();
        CompressedWriteBuffer<> compressed(*write_buffer, CompressionSettings(setting));

        WRITE_SEQUENCE(tests::SameValueGenerator(128));
        WRITE_SEQUENCE(tests::SequentialGenerator(2));
        WRITE_SEQUENCE(tests::SequentialGenerator(-2));
        WRITE_SEQUENCE(tests::MonotonicGenerator());
        WRITE_SEQUENCE(tests::MonotonicGenerator(-1));
        WRITE_SEQUENCE(tests::MinMaxGenerator());
        WRITE_SEQUENCE(tests::RandomGenerator<T>(0));
        WRITE_SEQUENCE(tests::RandomGenerator<T>(0, 0, 100));
        WRITE_SEQUENCE(tests::RepeatGenerator<T>(0));

        write_buffer->next();
        write_buffer->sync();
    }
    for (auto _ : state)
    {
        auto read_buffer = std::make_shared<ReadBufferFromFile>(file_name);
        CompressedReadBuffer<> compressed(*read_buffer);
        constexpr size_t buffer_size = 32 * 1024; // 32KB
        while (!compressed.eof())
        {
            char buffer[buffer_size];
            compressed.readBig(buffer, buffer_size);
            benchmark::DoNotOptimize(buffer);
        }
    }
    Poco::File(file_name).remove();
}

#define BENCH_MULTIPLE_WRITE_METHOD_TYPE(name, method, T) \
    static void name(benchmark::State & state)            \
    {                                                     \
        multipleWrite<method, T>(state);                  \
    }                                                     \
    BENCHMARK(name);

#define BENCH_MULTIPLE_READ_METHOD_TYPE(name, method, T) \
    static void name(benchmark::State & state)           \
    {                                                    \
        multipleRead<method, T>(state);                  \
    }                                                    \
    BENCHMARK(name);

#define BENCH_MULTIPLE_WRITE_TYPE(name, T)                                     \
    BENCH_MULTIPLE_WRITE_METHOD_TYPE(name##LZ4, CompressionMethodByte::LZ4, T) \
    BENCH_MULTIPLE_WRITE_METHOD_TYPE(name##Lightweight, CompressionMethodByte::Lightweight, T)

#define BENCH_MULTIPLE_READ_TYPE(name, T)                                     \
    BENCH_MULTIPLE_READ_METHOD_TYPE(name##LZ4, CompressionMethodByte::LZ4, T) \
    BENCH_MULTIPLE_READ_METHOD_TYPE(name##Lightweight, CompressionMethodByte::Lightweight, T)

#define BENCH_MULTIPLE_WRITE(name)                  \
    BENCH_MULTIPLE_WRITE_TYPE(name##UInt8, UInt8)   \
    BENCH_MULTIPLE_WRITE_TYPE(name##UInt16, UInt16) \
    BENCH_MULTIPLE_WRITE_TYPE(name##UInt32, UInt32) \
    BENCH_MULTIPLE_WRITE_TYPE(name##UInt64, UInt64)

#define BENCH_MULTIPLE_READ(name)                  \
    BENCH_MULTIPLE_READ_TYPE(name##UInt8, UInt8)   \
    BENCH_MULTIPLE_READ_TYPE(name##UInt16, UInt16) \
    BENCH_MULTIPLE_READ_TYPE(name##UInt32, UInt32) \
    BENCH_MULTIPLE_READ_TYPE(name##UInt64, UInt64)

BENCH_MULTIPLE_WRITE(CodecMultipleWrite)
BENCH_MULTIPLE_READ(CodecMultipleRead)

} // namespace DB::bench
