#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <gtest/gtest.h>
#pragma GCC diagnostic pop
#define TIFLASH_GTEST_ENABLE
#include <Encryption/PosixRandomAccessFile.h>
#include <Encryption/PosixWritableFile.h>
#include <Storages/DeltaMerge/File/Checksum/ChecksumBuffer.h>

#include <random>

namespace DB
{
namespace DM
{
namespace tests
{

namespace
{
std::pair<std::vector<char>, uint64_t> randomData(size_t size)
{
    std::vector<char>                   data(size);
    std::random_device                  dev;
    uint64_t                            seed = dev();
    std::mt19937_64                     eng(seed);
    std::uniform_int_distribution<char> dist{};
    for (auto & i : data)
    {
        i = dist(eng);
    }
    return {data, size};
}

constexpr char DM_CHECKSUM_BUFFER_TEST_PATH[] = "/tmp/tiflash_dm_checksum_buffer_gtest";

} // namespace

#define TEST_STREAM(ALGO)                                                                                         \
    TEST(DMChecksumBuffer, ALGO##Streaming)                                                                       \
    {                                                                                                             \
        for (auto size = 1; size <= 1024 * 1024; size <<= 1)                                                      \
        {                                                                                                         \
            auto [data, seed] = randomData(size);                                                                 \
            {                                                                                                     \
                                                                                                                  \
                auto pWTFile = std::make_shared<PosixWritableFile>(DM_CHECKSUM_BUFFER_TEST_PATH, true, -1, 0755); \
                auto buffer  = Checksum::FramedChecksumWriteBuffer<Digest::ALGO>(pWTFile);                        \
                buffer.write(data.data(), data.size());                                                           \
            }                                                                                                     \
                                                                                                                  \
            {                                                                                                     \
                                                                                                                  \
                auto pRAFile = std::make_shared<PosixRandomAccessFile>(DM_CHECKSUM_BUFFER_TEST_PATH, -1);         \
                auto buffer  = Checksum::FramedChecksumReadBuffer<Digest::ALGO>(pRAFile);                         \
                auto cmp     = std::vector<char>(size);                                                           \
                ASSERT_EQ(buffer.read(cmp.data(), size), size) << "random seed: " << seed;                        \
                ASSERT_EQ(data, cmp) << "random seed: " << seed;                                                  \
            }                                                                                                     \
        }                                                                                                         \
    }

TEST_STREAM(None)
TEST_STREAM(CRC32)
TEST_STREAM(CRC64)
TEST_STREAM(City128)

} // namespace tests
} // namespace DM
} // namespace DB