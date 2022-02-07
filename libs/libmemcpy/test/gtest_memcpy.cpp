#include <gtest/gtest.h>

#include <random>

#include "../memcpy.cpp"
using namespace memory_copy;

struct Guard
{
    MemcpyConfig config;
    Guard()
    {
        config = memcpy_config;
    }

    ~Guard()
    {
        memcpy_config = config;
    }
};

struct Memory
{
    size_t size;
    size_t alignment;
    size_t shift;
    char * data;


    Memory(size_t size, size_t alignment, size_t shift = 0)
        : size(size)
        , alignment(alignment)
        , shift(shift)
        , data(static_cast<char *>(::operator new (size + 2 * shift, std::align_val_t{alignment})) + shift)
    {
    }

    ~Memory()
    {
        ::operator delete (data - shift, std::align_val_t{alignment});
    }

    bool operator==(const Memory & that) const noexcept
    {
        return size == that.size && ::memcmp(data, that.data, size) == 0;
    }
};

size_t get_seed()
{
    return std::random_device{}();
}

void random_fill(Memory & memory, size_t seed)
{
    std::default_random_engine eng(seed);
    std::uniform_int_distribution<char> dist{};
    for (size_t i = 0; i < memory.size; ++i)
    {
        memory.data[i] = dist(eng);
    }
}

struct MemcpyMediumTest : public ::testing::TestWithParam<MediumSizeStrategy>
{
};
struct MemcpyHugeTest : public ::testing::TestWithParam<HugeSizeStrategy>
{
};

TEST(MemcpyTest, SmallSized)
{
    Guard guard;
    memcpy_config.medium_size_threshold = 2048;
    for (int i = 1; i < memcpy_config.medium_size_threshold; ++i)
    {
        {
            Memory src(i, 512), dst(i, 512);
            auto seed = get_seed();
            random_fill(src, seed);
            memcpy(dst.data, src.data, i);
            EXPECT_EQ(src, dst);
        }
        {
            Memory src(i, 512), dst(i, 512, 37);
            auto seed = get_seed();
            random_fill(src, seed);
            memcpy(dst.data, src.data, i);
            EXPECT_EQ(src, dst);
        }
    }
}

TEST_P(MemcpyMediumTest, Aligned)
{
    Guard guard;
    memcpy_config.medium_size_threshold = 2048;
    memcpy_config.huge_size_threshold = 0xc0000;
    if (check_valid_strategy(GetParam()))
    {
        memcpy_config.medium_size_strategy = GetParam();

        for (size_t i : {
                 2048,
                 2048 + 1,
                 2048 + 13,
                 2048 + 5261,
                 2038 + 16,
                 2048 + 128,
                 2048 + 256,
                 4096,
                 16384,
                 0xc0000 - 1})
        {
            Memory src(i, 512), dst(i, 512);
            auto seed = get_seed();
            random_fill(src, seed);
            memcpy(dst.data, src.data, i);
            EXPECT_EQ(src, dst);
        }
    }
}

TEST_P(MemcpyMediumTest, UnAligned)
{
    if (check_valid_strategy(GetParam()))
    {
        Guard guard;
        memcpy_config.medium_size_threshold = 2048;
        memcpy_config.huge_size_threshold = 0xc0000;
        memcpy_config.medium_size_strategy = GetParam();
        for (size_t i : {
                 2048,
                 2048 + 1,
                 2048 + 13,
                 2048 + 5261,
                 2038 + 16,
                 2048 + 128,
                 2048 + 256,
                 4096,
                 16384,
                 0xc0000 - 1})
        {
            Memory src(i, 512), dst(i, 512, 37);
            auto seed = get_seed();
            random_fill(src, seed);
            memcpy(dst.data, src.data, i);
            EXPECT_EQ(src, dst);
        }
    }
}

TEST_P(MemcpyHugeTest, Aligned)
{
    Guard guard;
    memcpy_config.huge_size_threshold = 0xc0000;
    if (check_valid_strategy(GetParam()))
    {
        memcpy_config.huge_size_strategy = GetParam();
        for (size_t i : {
                 0xc0000,
                 0xc0000 + 1,
                 0xc0000 * 2 + 1,
                 0xc0000 + 127,
                 0xc0000 + 128,
                 0xc0000 + 16,
                 0xc0000 * 17,
                 0xc0000 * 13,
                 0xc0000 * 64})
        {
            Memory src(i, 512), dst(i, 512);
            auto seed = get_seed();
            random_fill(src, seed);
            memcpy(dst.data, src.data, i);
            EXPECT_EQ(src, dst);
        }
    }
}

TEST_P(MemcpyHugeTest, UnAligned)
{
    Guard guard;
    memcpy_config.huge_size_threshold = 0xc0000;
    if (check_valid_strategy(GetParam()))
    {
        memcpy_config.huge_size_strategy = GetParam();
        for (size_t i : {
                 0xc0000,
                 0xc0000 + 1,
                 0xc0000 * 2 + 1,
                 0xc0000 + 127,
                 0xc0000 + 128,
                 0xc0000 + 16,
                 0xc0000 * 17,
                 0xc0000 * 13,
                 0xc0000 * 64})
        {
            Memory src(i, 512), dst(i, 512, 37);
            auto seed = get_seed();
            random_fill(src, seed);
            memcpy(dst.data, src.data, i);
            EXPECT_EQ(src, dst);
        }
    }
}

INSTANTIATE_TEST_CASE_P(
    MediumSizeStrategy,
    MemcpyMediumTest,
    ::testing::Values(
        MediumSizeStrategy::MediumSizeSSE,
        MediumSizeStrategy::MediumSizeRepMovsb));

INSTANTIATE_TEST_CASE_P(
    HugeSizeStrategy,
    MemcpyHugeTest,
    ::testing::Values(
        HugeSizeStrategy::HugeSizeSSE,
        HugeSizeStrategy::HugeSizeSSSE3Mux,
        HugeSizeStrategy::HugeSizeRepMovsb,
        HugeSizeStrategy::HugeSizeVEX32,
        HugeSizeStrategy::HugeSizeEVEX32,
        HugeSizeStrategy::HugeSizeEVEX64));