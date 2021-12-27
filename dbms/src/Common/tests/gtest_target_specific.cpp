#include <Common/TargetSpecific.h>
#include <gtest/gtest.h>

TIFLASH_DECLARE_MULTITARGET_FUNCTION(
    void,
    byteAddition,
    (src, dst, length),
    (const char * __restrict src, char * __restrict dst, size_t length),
    {
        for (size_t i = 0; i < length; ++i)
        {
            dst[i] = src[i] + 1;
        }
    })

TEST(TargetSpecific, byteAddition)
{
    srand(1024);
    std::vector<char> data(512);
    std::vector<char> result(512);
    for (auto & i : data)
    {
        i = rand() % 26 + 'a';
    }
    byteAddition(data.data(), result.data(), 512);
    for (size_t i = 0; i < result.size(); ++i)
    {
        auto x = result[i] - 1;
        TIFLASH_NO_OPTIMIZE(x);
        EXPECT_EQ(static_cast<int>(x), static_cast<int>(data[i]));
    }
}

TIFLASH_DECLARE_MULTITARGET_FUNCTION_ALONE(int, sumIntFromZero, (const int * __restrict src, size_t length))
TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION(int, sumIntFromZero, (src, length), (const int * __restrict src, size_t length), {
    int acc = 0;
    for (size_t i = 0; i < length; ++i)
    {
        acc += src[i];
    }
    return acc;
})

TEST(TargetSpecific, sumIntFromZero)
{
    size_t count = 512;
    std::vector<int> data(count);
    for (size_t i = 1; i <= count; ++i)
    {
        data[i - 1] = i;
    }
    auto res = sumIntFromZero(data.data(), 512);
    TIFLASH_NO_OPTIMIZE(res);
    EXPECT_EQ(static_cast<size_t>(res), count * (count + 1) / 2);
}
