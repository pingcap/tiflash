#include <Core/TargetSpecific.h>
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
        for (size_t i = 0; i < length; ++i)
        {
            auto x = dst[i] - 1;
            asm volatile(""
                         :
                         : "r,m"(x)
                         : "memory");
            ASSERT_EQ(static_cast<int>(x), static_cast<int>(src[i]));
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
}

TIFLASH_DECLARE_MULTITARGET_FUNCTION_ALONE(void, sumIntFromZero, (const int * __restrict src, size_t length))
TIFLASH_IMPLEMENT_MULTITARGET_FUNCTION(void, sumIntFromZero, (src, length), (const int * __restrict src, size_t length), {
    int acc = 0;
    for (size_t i = 0; i < length; ++i)
    {
        acc += src[i];
    }
    asm volatile(""
                 :
                 : "r,m"(acc)
                 : "memory");
    ASSERT_EQ(static_cast<size_t>(acc), length * (length + 1) / 2);
})

TEST(TargetSpecific, sumIntFromZero)
{
    std::vector<int> data(512);
    for (int i = 1; i <= 512; ++i)
    {
        data[i - 1] = i;
    }
    sumIntFromZero(data.data(), 512);
}
