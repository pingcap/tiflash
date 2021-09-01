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
            ASSERT_EQ(static_cast<int>(dst[i] - 1), static_cast<int>(src[i]));
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
