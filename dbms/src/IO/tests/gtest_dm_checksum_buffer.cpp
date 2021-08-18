#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <gtest/gtest.h>
#pragma GCC diagnostic pop
#include <Encryption/FileProvider.h>
#include <Encryption/MockKeyManager.h>
#include <Encryption/PosixRandomAccessFile.h>
#include <Encryption/PosixWritableFile.h>
#include <Encryption/RateLimiter.h>
#include <Encryption/createReadBufferFromFileBaseByFileProvider.h>
#include <IO/ChecksumBuffer.h>
#include <Poco/File.h>
#include <Storages/Page/PageUtil.h>

#include <random>

namespace DB
{
namespace tests
{

namespace
{

std::random_device dev;    // NOLINT(cert-err58-cpp)
uint64_t seed = dev();     // NOLINT(cert-err58-cpp)
std::mt19937_64 eng(seed); // NOLINT(cert-err58-cpp)

std::pair<std::vector<char>, uint64_t> randomData(size_t size)
{
    std::vector<char> data(size);
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
    int whence = dist(eng) & 1 ? SEEK_CUR : SEEK_SET;
    off_t offset;
    size_t length;
    off_t update;
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


constexpr char CHECKSUM_BUFFER_TEST_PATH[] = "/tmp/tiflash_dm_checksum_buffer_gtest";

auto prepareIO()
{
    auto rate_limiter = std::make_shared<DB::IORateLimiter>();
    auto key_manager = std::make_shared<DB::MockKeyManager>();
    auto file_provider = std::make_shared<DB::FileProvider>(key_manager, true);
    return std::make_pair(std::move(rate_limiter), std::move(file_provider));
}

} // namespace

#define TEST_STREAM(ALGO) \
    TEST(ChecksumBuffer, ALGO##Streaming) { runStreamingTest<Digest::ALGO>(); } // NOLINT(cert-err58-cpp)

template <class D>
void runStreamingTest()
{
    for (auto size = 1; size <= 1024 * 1024; size <<= 1)
    {
        auto [data, seed] = randomData(size);
        {

            auto writable_file_ptr = std::make_shared<PosixWritableFile>(CHECKSUM_BUFFER_TEST_PATH, true, -1, 0755);
            auto buffer = DB::FramedChecksumWriteBuffer<D>(writable_file_ptr);
            buffer.write(data.data(), data.size());
        }

        {

            auto readable_file_ptr = std::make_shared<PosixRandomAccessFile>(CHECKSUM_BUFFER_TEST_PATH, -1);
            auto buffer = DB::FramedChecksumReadBuffer<D>(readable_file_ptr);
            auto cmp = std::vector<char>(size);
            ASSERT_EQ(buffer.read(cmp.data(), size), size) << "random seed: " << seed << std::endl;
            ASSERT_EQ(data, cmp) << "random seed: " << seed << std::endl;
        }
    }
    Poco::File file{CHECKSUM_BUFFER_TEST_PATH};
    file.remove();
}

TEST_STREAM(None)
TEST_STREAM(CRC32)
TEST_STREAM(CRC64)
TEST_STREAM(City128)
TEST_STREAM(XXH3)

#define TEST_SEEK(ALGO) \
    TEST(ChecksumBuffer, ALGO##Seeking) { runSeekingTest<Digest::ALGO>(); } // NOLINT(cert-err58-cpp)

template <class D>
void runSeekingTest()
{
    for (auto size = 1024; size <= 4096 * 1024; size <<= 1)
    {
        auto [data, seed] = randomData(size);
        {
            auto writable_file_ptr = std::make_shared<PosixWritableFile>(CHECKSUM_BUFFER_TEST_PATH, true, -1, 0755);
            auto buffer = DB::FramedChecksumWriteBuffer<D>(writable_file_ptr);
            buffer.write(data.data(), data.size());
        }
        {
            auto readable_file_ptr = std::make_shared<PosixRandomAccessFile>(CHECKSUM_BUFFER_TEST_PATH, -1);
            auto buffer = DB::FramedChecksumReadBuffer<D>(readable_file_ptr);
            off_t current = 0;
            for (auto i = 0; i < 1024; ++i)
            {
                auto [offset, whence, length, next] = randomOperation(size, current);
                current = next;
                buffer.seek(offset, whence);
                ASSERT_EQ(current, buffer.getPositionInFile());
                std::vector<char> data_slice(length);
                std::vector<char> file_slice(length);
                std::copy(data.begin() + current, data.begin() + current + static_cast<off_t>(length), data_slice.begin());
                buffer.read(file_slice.data(), length);
                ASSERT_EQ(data_slice, file_slice) << "seed: " << seed << "size: " << size << ", whence: " << whence << ", off: " << offset
                                                  << ", pos: " << current << ", length: " << length << std::endl;
                current += static_cast<off_t>(length);
                ASSERT_EQ(current, buffer.getPositionInFile());
            }
        };
    }
    Poco::File file{CHECKSUM_BUFFER_TEST_PATH};
    file.remove();
}

TEST_SEEK(None)
TEST_SEEK(CRC32)
TEST_SEEK(CRC64)
TEST_SEEK(City128)
TEST_SEEK(XXH3)

template <class D>
void runReadBigTest()
{
    auto [limiter, provider] = prepareIO();
    size_t size = 1024 * 1024 * 4;
    auto [data, seed] = randomData(size);
    auto compare = data;
    {
        auto file = provider->newWritableFile("/tmp/test", {"/tmp/test.enc", "test.enc"}, true, true, limiter->getWriteLimiter());
        auto buffer = FramedChecksumWriteBuffer<D>(file);
        buffer.write(data.data(), data.size());
    }
    {
        auto file = provider->newRandomAccessFile("/tmp/test", {"/tmp/test.enc", "test.enc"}, limiter->getReadLimiter());
        auto buffer = FramedChecksumReadBuffer<D>(file);
        buffer.readBig(compare.data(), compare.size());
        ASSERT_EQ(std::memcmp(compare.data(), data.data(), data.size()), 0) << "seed: " << seed;
    }

    for (size_t i = 1; i <= data.size() / 2; i <<= 1)
    {
        auto file = provider->newRandomAccessFile("/tmp/test", {"/tmp/test.enc", "test.enc"}, limiter->getReadLimiter());
        auto buffer = FramedChecksumReadBuffer<D>(file);
        buffer.seek(static_cast<ssize_t>(i));
        buffer.readBig(compare.data(), i);
        ASSERT_EQ(std::memcmp(compare.data(), data.data() + i, i), 0) << "seed: " << seed;
    }
    Poco::File file{"/tmp/test"};
    file.remove();
}

#define TEST_BIG_READING(ALGO) \
    TEST(ChecksumBuffer, ALGO##BigReading) { runReadBigTest<DB::Digest::ALGO>(); } // NOLINT(cert-err58-cpp)

TEST_BIG_READING(None)
TEST_BIG_READING(CRC32)
TEST_BIG_READING(CRC64)
TEST_BIG_READING(City128)
TEST_BIG_READING(XXH3)


} // namespace tests
} // namespace DB
