// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/Decimal.h>
#include <Common/SipHash.h>
#include <Core/Field.h>
#include <gtest/gtest.h>

namespace DB
{
namespace tests
{
template <typename T>
std::enable_if_t<is_integer_v<T> || std::is_floating_point_v<T> || std::is_same_v<T, Int128>, void> test_siphash(
    T & value)
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
