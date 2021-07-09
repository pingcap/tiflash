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

std::random_device dev;          // NOLINT(cert-err58-cpp)
uint64_t           seed = dev(); // NOLINT(cert-err58-cpp)
std::mt19937_64    eng(seed);    // NOLINT(cert-err58-cpp)

std::pair<std::vector<char>, uint64_t> randomData(size_t size)
{
    std::vector<char>                   data(size);
    std::uniform_int_distribution<char> dist{};
    for (auto & i : data)
    {
        i = dist(eng);
    }
    return {data, size};
}

// seek offset, whence, read length, update offset
std::tuple<off_t, int, size_t, off_t> randomOperation(size_t size, off_t current)
{
    std::uniform_int_distribution<off_t> dist{};
    int                                  whence = dist(eng) & 1 ? SEEK_CUR : SEEK_SET;
    off_t                                offset;
    size_t                               length;
    off_t                                update;
    if (whence == SEEK_SET)
    {
        std::uniform_int_distribution<off_t> pos{0, static_cast<off_t>(size)};
        offset = pos(eng);
        update = offset;
        std::uniform_int_distribution<off_t> len{0, static_cast<off_t>(size) - update};
        length = len(eng);
    }
    else
    {
        std::uniform_int_distribution<off_t> delta{-current, static_cast<off_t>(size) - current};
        offset = delta(eng);
        update = current + offset;
        std::uniform_int_distribution<off_t> len{0, static_cast<off_t>(size) - update};
        length = len(eng);
    }

    return {offset, whence, length, update};
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
                ASSERT_EQ(buffer.read(cmp.data(), size), size) << "random seed: " << seed << std::endl;           \
                ASSERT_EQ(data, cmp) << "random seed: " << seed << std::endl;                                     \
            }                                                                                                     \
        }                                                                                                         \
    }

TEST_STREAM(None)
TEST_STREAM(CRC32)
TEST_STREAM(CRC64)
TEST_STREAM(City128)
TEST_STREAM(XXH3)

#define TEST_SEEK(ALGO)                                                                                                                  \
    TEST(DMChecksumBuffer, ALGO##Seeking)                                                                                                \
    {                                                                                                                                    \
        for (auto size = 1024; size <= 4096 * 1024; size <<= 1)                                                                          \
        {                                                                                                                                \
            auto [data, seed] = randomData(size);                                                                                        \
            {                                                                                                                            \
                auto pWTFile = std::make_shared<PosixWritableFile>(DM_CHECKSUM_BUFFER_TEST_PATH, true, -1, 0755);                        \
                auto buffer  = Checksum::FramedChecksumWriteBuffer<Digest::ALGO>(pWTFile);                                               \
                buffer.write(data.data(), data.size());                                                                                  \
            }                                                                                                                            \
            {                                                                                                                            \
                for (auto i = 0; i < 1024; ++i)                                                                                          \
                {                                                                                                                        \
                    off_t current                       = 0;                                                                             \
                    auto  pRAFile                       = std::make_shared<PosixRandomAccessFile>(DM_CHECKSUM_BUFFER_TEST_PATH, -1);     \
                    auto  buffer                        = Checksum::FramedChecksumReadBuffer<Digest::ALGO>(pRAFile);                     \
                    auto [offset, whence, length, next] = randomOperation(size, current);                                                \
                    current                             = next;                                                                          \
                    buffer.seek(offset, whence);                                                                                         \
                    ASSERT_EQ(current, buffer.getPositionInFile());                                                                      \
                    std::vector<char> data_slice(length);                                                                                \
                    std::vector<char> file_slice(length);                                                                                \
                    std::copy(data.begin() + current, data.begin() + current + length, data_slice.begin());                              \
                    buffer.read(file_slice.data(), length);                                                                              \
                    ASSERT_EQ(data_slice, file_slice)                                                                                    \
                        << "seed: " << seed << "size: " << size << ", whence: " << whence << ", off: " << offset << ", pos: " << current \
                        << ", length: " << length << std::endl;                                                                          \
                }                                                                                                                        \
            };                                                                                                                           \
        }                                                                                                                                \
    }

TEST_SEEK(None)
TEST_SEEK(CRC32)
TEST_SEEK(CRC64)
TEST_SEEK(City128)
TEST_SEEK(XXH3)

} // namespace tests
} // namespace DM
} // namespace DB