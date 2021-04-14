#include <Common/Decimal.h>
#include <Common/SipHash.h>
#include <Core/Field.h>
#include <gtest/gtest.h>

namespace DB
{
namespace tests
{

template <typename T>
std::enable_if_t<std::is_integral_v<T> || std::is_floating_point_v<T> || DB::IsBoostNumber<T> || std::is_same_v<T, Int128>, void> test_siphash(T & value)
{
    auto hash_value = sipHash64(value);
    value += 1;
    value -= 1;
    ASSERT_TRUE(hash_value == sipHash64(value));
    SipHash hash;
    hash.update(value);
    ASSERT_TRUE(hash_value == hash.get64());
}

template <typename T>
std::enable_if_t<DB::IsDecimal<T>, void> test_siphash(T & value)
{
    auto hash_value = sipHash64(value);
    value.value += 1;
    value.value -= 1;
    ASSERT_TRUE(hash_value == sipHash64(value));
    SipHash hash;
    hash.update(value);
    ASSERT_TRUE(hash_value == hash.get64());
}

template <typename T>
std::enable_if_t<std::is_same_v<T, String>, void> test_siphash(T & value)
{
    auto hash_value = sipHash64(value);
    T new_value = value;
    ASSERT_TRUE(hash_value == sipHash64(new_value));
    SipHash hash;
    hash.update(new_value.data(), new_value.size());
    ASSERT_TRUE(hash_value == hash.get64());
}

TEST(SipHash_test, test)
{
#define APPLY_FOR_SIGNED_TYPES(M) \
    M(Int8)                       \
    M(Int16)                      \
    M(Int32)                      \
    M(Int64)                      \
    M(Int128)                     \
    M(Int256)                     \
    M(Int512)                     \
    M(Float32)                    \
    M(Float64)                    \
    M(Decimal32)                  \
    M(Decimal64)                  \
    M(Decimal128)


#define M(TYPE)                \
    TYPE value##TYPE = 111;    \
    test_siphash(value##TYPE); \
    value##TYPE = -111;        \
    test_siphash(value##TYPE); \
    value##TYPE = 0;           \
    test_siphash(value##TYPE);
    APPLY_FOR_SIGNED_TYPES(M)
#undef M

#define APPLY_FOR_UNSIGNED_TYPES(M) \
    M(UInt8)                        \
    M(UInt16)                       \
    M(UInt32)                       \
    M(UInt64)


#define M(TYPE)                \
    TYPE value##TYPE = 111;    \
    test_siphash(value##TYPE); \
    value##TYPE = 0;           \
    test_siphash(value##TYPE);
    APPLY_FOR_UNSIGNED_TYPES(M)
#undef M

    Decimal256 d_256 = Int256(111);
    test_siphash(d_256);
    d_256 = Int256(0);
    test_siphash(d_256);
    d_256 = Int256(-111);
    test_siphash(d_256);

    String value = "111";
    test_siphash(value);
    value = "";
    test_siphash(value);
}

} // namespace tests
} // namespace DB
