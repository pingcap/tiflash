#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <gtest/gtest.h>
#pragma GCC diagnostic pop
#define TIFLASH_GTEST_ENABLE
#include <Common/TiFlashMetrics.h>
#include <Encryption/CompressedReadBufferFromFileProvider.h>
#include <Encryption/FileProvider.h>
#include <Encryption/MockKeyManager.h>
#include <Encryption/PosixRandomAccessFile.h>
#include <Encryption/PosixWritableFile.h>
#include <Encryption/RateLimiter.h>
#include <Encryption/createReadBufferFromFileBaseByFileProvider.h>
#include <Encryption/createWriteBufferFromFileBaseByFileProvider.h>
#include <IO/CompressedWriteBuffer.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/File/Checksum/ChecksumBuffer.h>
#include <Storages/Page/PageUtil.h>

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

auto prepareIO()
{
    auto metric        = std::make_shared<DB::TiFlashMetrics>();
    auto rate_limiter  = std::make_shared<DB::RateLimiter>(metric, 0);
    auto key_manager   = std::make_shared<DB::MockKeyManager>();
    auto file_provider = std::make_shared<DB::FileProvider>(key_manager, true);
    return std::make_pair(std::move(rate_limiter), std::move(file_provider));
}

} // namespace

#define TEST_STREAM(ALGO) \
    TEST(DMChecksumBuffer, ALGO##Streaming) { runStreamingTest<Digest::ALGO>(); } // NOLINT(cert-err58-cpp)

template <class D>
void runStreamingTest()
{
    for (auto size = 1; size <= 1024 * 1024; size <<= 1)
    {
        auto [data, seed] = randomData(size);
        {

            auto writable_file_ptr = std::make_shared<PosixWritableFile>(DM_CHECKSUM_BUFFER_TEST_PATH, true, -1, 0755);
            auto buffer            = Checksum::FramedChecksumWriteBuffer<D>(writable_file_ptr);
            buffer.write(data.data(), data.size());
        }

        {

            auto readable_file_ptr = std::make_shared<PosixRandomAccessFile>(DM_CHECKSUM_BUFFER_TEST_PATH, -1);
            auto buffer            = Checksum::FramedChecksumReadBuffer<D>(readable_file_ptr);
            auto cmp               = std::vector<char>(size);
            ASSERT_EQ(buffer.read(cmp.data(), size), size) << "random seed: " << seed << std::endl;
            ASSERT_EQ(data, cmp) << "random seed: " << seed << std::endl;
        }
    }
    Poco::File file{DM_CHECKSUM_BUFFER_TEST_PATH};
    file.remove();
}

TEST_STREAM(None)
TEST_STREAM(CRC32)
TEST_STREAM(CRC64)
TEST_STREAM(City128)
TEST_STREAM(XXH3)

#define TEST_SEEK(ALGO) \
    TEST(DMChecksumBuffer, ALGO##Seeking) { runSeekingTest<Digest::ALGO>(); } // NOLINT(cert-err58-cpp)

template <class D>
void runSeekingTest()
{
    for (auto size = 1024; size <= 4096 * 1024; size <<= 1)
    {
        auto [data, seed] = randomData(size);
        {
            auto writable_file_ptr = std::make_shared<PosixWritableFile>(DM_CHECKSUM_BUFFER_TEST_PATH, true, -1, 0755);
            auto buffer            = Checksum::FramedChecksumWriteBuffer<D>(writable_file_ptr);
            buffer.write(data.data(), data.size());
        }
        {
            auto  readable_file_ptr = std::make_shared<PosixRandomAccessFile>(DM_CHECKSUM_BUFFER_TEST_PATH, -1);
            auto  buffer            = Checksum::FramedChecksumReadBuffer<D>(readable_file_ptr);
            off_t current           = 0;
            for (auto i = 0; i < 1024; ++i)
            {
                auto [offset, whence, length, next] = randomOperation(size, current);
                current                             = next;
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
    Poco::File file{DM_CHECKSUM_BUFFER_TEST_PATH};
    file.remove();
}

TEST_SEEK(None)
TEST_SEEK(CRC32)
TEST_SEEK(CRC64)
TEST_SEEK(City128)
TEST_SEEK(XXH3)


template <ChecksumAlgo D>
void runStackingTest()
{
    auto [limiter, provider] = prepareIO();
    auto config              = DMConfiguration{{}, TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE, D};
    for (auto size = 1024; size <= 4096 * 1024; size <<= 1)
    {
        auto [data, seed] = randomData(size);
        {
            auto buffer
                = createWriteBufferFromFileBaseByFileProvider(provider, "/tmp/test", {"/tmp/test.enc", "test.enc"}, true, limiter, config);
            auto compression_buffer = CompressedWriteBuffer<false>(*buffer);
            compression_buffer.write(data.data(), data.size());
        }
        {
            auto buffer = CompressedReadBufferFromFileProvider<false>(provider, "/tmp/test", {"/tmp/test.enc", "test.enc"}, config);
            auto cmp    = std::vector<char>(size);
            ASSERT_EQ(buffer.read(cmp.data(), size), size) << "random seed: " << seed << std::endl;
            ASSERT_EQ(data, cmp) << "random seed: " << seed << std::endl;
        }
    }
    Poco::File file{"/tmp/test"};
    file.remove();
}

#define TEST_STACKING(ALGO) \
    TEST(DMChecksumBuffer, ALGO##Stacking) { runStackingTest<ChecksumAlgo::ALGO>(); } // NOLINT(cert-err58-cpp)

TEST_STACKING(None)
TEST_STACKING(CRC32)
TEST_STACKING(CRC64)
TEST_STACKING(City128)
TEST_STACKING(XXH3)


template <ChecksumAlgo D>
void runStackedSeekingTest()
{
    auto local_engine                                                 = std::mt19937_64{seed};
    auto [limiter, provider]                                          = prepareIO();
    auto                                                       config = DMConfiguration{{}, TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE, D};
    size_t                                                     size   = 1024 * 1024 * 1024;
    std::vector<std::tuple<std::vector<char>, size_t, size_t>> slices;
    auto [data, seed] = randomData(size);
    {
        auto buffer
            = createWriteBufferFromFileBaseByFileProvider(provider, "/tmp/test", {"/tmp/test.enc", "test.enc"}, true, limiter, config);
        auto   compression_buffer = CompressedWriteBuffer<false>(*buffer);
        size_t acc                = 0;
        for (size_t length = 1; acc + length <= size; acc += length, length <<= 1)
        {
            std::vector<char> slice;
            slice.resize(length);
            std::copy(data.begin() + acc, data.begin() + acc + length, slice.begin());
            if (local_engine() & 1)
            {
                compression_buffer.next();
            }
            auto x = buffer->count();             // compressed position
            auto y = compression_buffer.offset(); // uncompressed position
            compression_buffer.write(slice.data(), slice.size());
            slices.template emplace_back(std::move(slice), x, y);
        }
    }
    {
        auto buffer = CompressedReadBufferFromFileProvider<false>(provider, "/tmp/test", {"/tmp/test.enc", "test.enc"}, config);
        std::shuffle(slices.begin(), slices.end(), local_engine);
        for (const auto & [x, y, z] : slices)
        {
            buffer.seek(y, z);
            auto cmp = std::vector<char>(x.size());
            ASSERT_EQ(buffer.read(cmp.data(), cmp.size()), cmp.size()) << "random seed: " << seed << std::endl;
            ASSERT_EQ(x, cmp) << "random seed: " << seed << std::endl;
        }
    }
    Poco::File file{"/tmp/test"};
    file.remove();
}

#define TEST_STACKED_SEEKING(ALGO) \
    TEST(DMChecksumBuffer, ALGO##StackedSeeking) { runStackedSeekingTest<ChecksumAlgo::ALGO>(); } // NOLINT(cert-err58-cpp)

TEST_STACKED_SEEKING(None)
TEST_STACKED_SEEKING(CRC32)
TEST_STACKED_SEEKING(CRC64)
TEST_STACKED_SEEKING(City128)
TEST_STACKED_SEEKING(XXH3)

template <ChecksumAlgo D>
void runLargeReadingTest()
{
    auto local_engine        = std::mt19937_64{seed};
    auto [limiter, provider] = prepareIO();
    auto   config            = DMConfiguration{{}, TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE, D};
    size_t size              = 1024 * 1024 * 4;
    auto [data, seed]        = randomData(size);
    {
        auto buffer
            = createWriteBufferFromFileBaseByFileProvider(provider, "/tmp/test", {"/tmp/test.enc", "test.enc"}, true, limiter, config);
        buffer->write(data.data(), data.size());
    }
    {
        auto file = provider->newRandomAccessFile("/tmp/test", {"/tmp/test.enc", "test.enc"});
        for (auto trial = 0; trial < 10000; ++trial)
        {
            auto              offset = std::uniform_int_distribution<off_t>(0, static_cast<off_t>(size))(local_engine);
            auto              length = std::uniform_int_distribution<off_t>(0, static_cast<off_t>(size) - offset)(local_engine);
            std::vector<char> buffer(length);
            PageUtil::readChecksumFramedFile(file, offset, buffer.data(), length, config);
            ASSERT_EQ(std::memcmp(buffer.data(), data.data() + offset, length), 0) << "seed: " << seed;
        }
    }
    Poco::File file{"/tmp/test"};
    file.remove();
}

#define TEST_LARGE_READING(ALGO) \
    TEST(DMChecksumBuffer, ALGO##LargeReading) { runLargeReadingTest<ChecksumAlgo::ALGO>(); } // NOLINT(cert-err58-cpp)

TEST_LARGE_READING(None)
TEST_LARGE_READING(CRC32)
TEST_LARGE_READING(CRC64)
TEST_LARGE_READING(City128)
TEST_LARGE_READING(XXH3)


} // namespace tests
} // namespace DM
} // namespace DB