//
// Created by schrodinger on 6/29/22.
//
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <random>
#include <vector>

struct ReadBuffer
{
    FILE * input;
    std::vector<char> buffer;
    char * current;
    char * end;

    ReadBuffer(const char * path, size_t buffer_size)
        : input(::fopen(path, "r"))
        , buffer(buffer_size)
        , current(buffer.data() + buffer_size)
        , end(buffer.data() + buffer_size)
    {
    }

    void nextFrame()
    {
        auto count = ::fread(buffer.data(), 1, buffer.size(), input);
        current = buffer.data();
        end = current + count;
    }

    void read(char * dst, size_t size)
    {
        while (size)
        {
            if (current >= end)
            {
                nextFrame();
            }
            auto length = std::min(static_cast<size_t>(end - current), size);
            ::memcpy(dst, current, length);
            current += length;
            dst += length;
            size -= length;
        }
    }

    ~ReadBuffer()
    {
        ::fclose(input);
    }
};

template <size_t BLOCK_SIZE>
static __attribute__((noinline)) inline void deserializeBinaryBlockImpl(ReadBuffer & input, std::vector<size_t> & offsets, std::vector<char> & data, size_t limit)
{
    size_t offset = data.size();

    for (size_t i = 0; i < limit; ++i)
    {
        size_t size;
        input.read(reinterpret_cast<char *>(&size), sizeof(size));
        offset += size + 1;
        offsets.push_back(offset);
        data.resize(offset);

#ifdef ERMS
        // The intel reference manual states that for sizes larger than 128, using REP MOVSB will give identical performance with other variants.
        // Beginning from 2048 bytes, REP MOVSB gives an even better performance.
        if constexpr (BLOCK_SIZE >= 256)
        {
            /*
             * According to intel's reference manual:
             *
             * On older microarchitecture (ivy bridge), a REP MOVSB implementation of memcpy can achieve throughput at
             * slightly better than the 128-bit SIMD implementation when copying thousands of bytes.
             *
             * On newer microarchitecture (haswell), using REP MOVSB to implement memcpy operation for large copy length
             * can take advantage the 256-bit store data path and deliver throughput of more than 20 bytes per cycle.
             * For copy length that are smaller than a few hundred bytes, REP MOVSB approach is still slower than those
             * SIMD approaches.
             */
            if (size >= 1024 && input.current + size <= input.end)
            {
                const auto * src = reinterpret_cast<const char *>(input.current);
                auto * dst = reinterpret_cast<char *>(&data[offset - size - 1]);
                input.current += size;
                /*
                 *  For destination buffer misalignment:
                 *  The impact on Enhanced REP MOVSB and STOSB implementation can be 25%
                 *  degradation, while 128-bit AVX implementation of memcpy may degrade only
                 *  5%, relative to 16-byte aligned scenario.
                 *
                 *  Therefore, we manually align up the destination buffer before startup.
                 */
                __builtin_memcpy_inline(dst, src, 64);
                auto address = reinterpret_cast<uintptr_t>(dst);
                auto shift = 64ull - (address % 64ull);
                dst += shift;
                src += shift;
                size -= shift;
                asm volatile("rep movsb"
                             : "+D"(dst), "+S"(src), "+c"(size)
                             :
                             : "memory");
                data[offset - 1] = 0;
                continue;
            }
        }
#endif

        if (size)
        {
#ifdef OPTIMISTIC_BRANCH
            /// An optimistic branch in which more efficient copying is possible.
            if (offset + BLOCK_SIZE <= data.capacity() && input.current + size + BLOCK_SIZE <= input.end)
            {
                const auto * src = reinterpret_cast<const char *>(input.current);
                const auto * target = src + (size + BLOCK_SIZE - 1) / BLOCK_SIZE * BLOCK_SIZE;
                auto * dst = reinterpret_cast<char *>(&data[offset - size - 1]);

                while (src < target)
                {
                    /**
                     * Compiler expands the builtin memcpy perfectly. There is no need to
                     * manually write SSE2 code for x86 here; moreover, this method can also bring
                     * optimization to aarch64 targets.
                     *
                     * (x86 loop body for 64 bytes version with XMM registers)
                     *     movups  xmm0, xmmword ptr [rsi]
                     *     movups  xmm1, xmmword ptr [rsi + 16]
                     *     movups  xmm2, xmmword ptr [rsi + 32]
                     *     movups  xmm3, xmmword ptr [rsi + 48]
                     *     movups  xmmword ptr [rdi + 48], xmm3
                     *     movups  xmmword ptr [rdi + 32], xmm2
                     *     movups  xmmword ptr [rdi + 16], xmm1
                     *     movups  xmmword ptr [rdi], xmm0
                     *
                     * (aarch64 loop body for 64 bytes version with Q registers)
                     *     ldp     q0, q1, [x1]
                     *     ldp     q2, q3, [x1, #32]
                     *     stp     q0, q1, [x0]
                     *     add     x1, x1, #64
                     *     cmp     x1, x8
                     *     stp     q2, q3, [x0, #32]
                     *     add     x0, x0, #64
                     */
                    __builtin_memcpy_inline(dst, src, BLOCK_SIZE);
                    src += BLOCK_SIZE;
                    dst += BLOCK_SIZE;
                }
                input.current += size;
            }
            else
#endif
            {
                input.read(&data[offset - size - 1], size);
            }
        }

        data[offset - 1] = 0;
    }
}


void generate(const char * output, size_t count, size_t limit, uint64_t seed)
{
    std::default_random_engine eng(seed);
    std::normal_distribution<double> dist{0.5, 0.16};
    auto double_limit = static_cast<double>(limit);
    auto * out = ::fopen(output, "w");
    std::vector<char> data;
    for (size_t i = 0; i < count; ++i)
    {
        auto length = static_cast<size_t>(dist(eng) * double_limit);
        length = std::min(std::max(size_t{0}, length), limit);
        data.resize(length, '@');
        ::fwrite(&length, 1, sizeof(length), out);
        ::fwrite(data.data(), 1, data.size(), out);
    }
    ::fclose(out);
}

static const uint64_t seed = 0x12345678ull;
static const size_t count = 10000;
static const size_t buffer_size = 1048576ULL;

void bench(size_t average)
{
    std::cout << "benchmarking for strings with average length: " << average << ", ";
    std::cout.flush();

    generate("/tmp/data-test", count, average * 2, seed);
    auto buffer = ReadBuffer{"/tmp/data-test", buffer_size};
    std::vector<char> data;
    std::vector<size_t> offsets;

    auto start = __builtin_readcyclecounter();
    if (average >= 256)
    {
        deserializeBinaryBlockImpl<256>(buffer, offsets, data, count);
    }
    else if (average >= 128)
    {
        deserializeBinaryBlockImpl<128>(buffer, offsets, data, count);
    }
    else if (average >= 64)
    {
        deserializeBinaryBlockImpl<64>(buffer, offsets, data, count);
    }
    else if (average >= 48)
    {
        deserializeBinaryBlockImpl<48>(buffer, offsets, data, count);
    }
    else if (average >= 32)
    {
        deserializeBinaryBlockImpl<32>(buffer, offsets, data, count);
    }
    else
    {
        deserializeBinaryBlockImpl<16>(buffer, offsets, data, count);
    }

    auto end = __builtin_readcyclecounter();
    std::cout << "cycle count: " << end - start << std::endl;
}

int main()
{
    bench(8);
    bench(16);
    bench(32);
    bench(64);
    bench(128);
    bench(256);
    bench(512);
    bench(1024);
    bench(2048);
    bench(4096);
    bench(16384);
}
