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

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <random>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <Poco/Types.h>

#pragma GCC diagnostic pop

namespace DB
{
namespace tests
{
class TestInetAtonNtoa : public DB::tests::FunctionTest
{
protected:
    using DataVectorString = std::vector<std::optional<String>>;

    static String toBinary(UInt32 v)
    {
        char bytes[4];
        bytes[3] = static_cast<char>(v & 0xff);
        v >>= 8;
        bytes[2] = static_cast<char>(v & 0xff);
        v >>= 8;
        bytes[1] = static_cast<char>(v & 0xff);
        v >>= 8;
        bytes[0] = static_cast<char>(v & 0xff);
        return String(bytes, 4);
    }

    static String toBinary(const std::vector<UInt32> & vec)
    {
        String s;
        for (auto v : vec)
            s.append(toBinary(v));
        return s;
    }

    static DataVectorString toBinariesV4(const std::vector<UInt32> & vec)
    {
        DataVectorString res;
        for (const auto & v : vec)
            res.emplace_back(toBinary(v));
        return res;
    }

    static DataVectorString toBinariesV6(const std::vector<std::vector<UInt32>> & vec)
    {
        DataVectorString res;
        for (const auto & v : vec)
            res.emplace_back(toBinary(v));
        return res;
    }
};

TEST_F(TestInetAtonNtoa, InetAton)
try
{
    const String func_name = "tiDBIPv4StringToNum";

    // empty column
    ASSERT_COLUMN_EQ(createColumn<Nullable<UInt32>>({}), executeFunction(func_name, createColumn<String>({})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt32>>({}),
        executeFunction(func_name, createColumn<Nullable<String>>({})));

    // const null-only column
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<UInt32>>(1, {}),
        executeFunction(func_name, createConstColumn<Nullable<String>>(1, {})));

    // const non-null column
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<UInt32>>(1, 1),
        executeFunction(func_name, createConstColumn<Nullable<String>>(1, "0.0.0.1")));

    // normal valid cases
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt32>>({16909060, 65537, 16711935, 66051, 0, 16777472, 1862276364}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>(
                {"1.2.3.4",
                 "0.1.0.1",
                 "0.255.0.255",
                 "0000.1.2.3",
                 "00000.0000.0000.000",
                 "1.0.1.0",
                 "111.0.21.012"})));

    // valid but weird cases
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt32>>({255, 255, 255, 255, 65535, 16711935, 16711935, 1, 16777218, 16908291}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>(
                {"255", ".255", "..255", "...255", "..255.255", ".255.255", ".255..255", "1", "1.2", "1.2.3"})));

    // invalid cases
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt32>>({{}, {}, {}, {}, {}, {}, {}, {}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>(
                {{}, "", ".", "....255", "...255.255", ".255...255", ".255.255.", "1.0.a", "1.a", "a.1"})));
}
CATCH


template <typename Type>
static void TestInetAtonNtoaImpl(TestInetAtonNtoa & test)
{
    const String func_name = "IPv4NumToString";

    // empty column
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({}),
        test.executeFunction(func_name, createColumn<Nullable<Type>>({})));

    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>({}), test.executeFunction(func_name, createColumn<Type>({})));

    // const null-only column
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(1, {}),
        test.executeFunction(func_name, createConstColumn<Nullable<Type>>(1, {})));

    if constexpr (std::is_same_v<UInt8, Type>)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"0.0.0.255"}),
            test.executeFunction(func_name, createColumn<Nullable<Type>>({std::numeric_limits<Type>::max()})));
    }
    else if constexpr (std::is_same_v<Int8, Type>)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({{}, "0.0.0.127"}),
            test.executeFunction(func_name, createColumn<Nullable<Type>>({-1, std::numeric_limits<Type>::max()})));
    }
    else if constexpr (std::is_same_v<UInt16, Type>)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"0.0.255.255"}),
            test.executeFunction(func_name, createColumn<Nullable<Type>>({std::numeric_limits<Type>::max()})));
    }
    else if constexpr (std::is_same_v<Int16, Type>)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({{}, "0.0.127.255"}),
            test.executeFunction(func_name, createColumn<Nullable<Type>>({-1, std::numeric_limits<Type>::max()})));
    }
    else if constexpr (std::is_same_v<UInt32, Type>)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"255.255.255.255"}),
            test.executeFunction(func_name, createColumn<Nullable<Type>>({std::numeric_limits<Type>::max()})));
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>(
                {"1.2.3.4", "0.1.0.1", "0.255.0.255", "0.1.2.3", "0.0.0.0", "1.0.1.0", "111.0.21.12"}),
            test.executeFunction(
                func_name,
                createColumn<Nullable<Type>>({16909060, 65537, 16711935, 66051, 0, 16777472, 1862276364})));
    }
    else if constexpr (std::is_same_v<Int32, Type>)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({{}, "127.255.255.255"}),
            test.executeFunction(func_name, createColumn<Nullable<Type>>({-1, std::numeric_limits<Type>::max()})));
    }
    else if constexpr (std::is_same_v<UInt64, Type>)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"255.255.255.255", {}}),
            test.executeFunction(
                func_name,
                createColumn<Nullable<Type>>({std::numeric_limits<UInt32>::max(), std::numeric_limits<Type>::max()})));
    }
    else if constexpr (std::is_same_v<Int64, Type>)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"255.255.255.255", {}, {}}),
            test.executeFunction(
                func_name,
                createColumn<Nullable<Type>>(
                    {std::numeric_limits<UInt32>::max(), -1, std::numeric_limits<Type>::max()})));
    }
}


TEST_F(TestInetAtonNtoa, InetNtoa)
try
{
#define M(T) TestInetAtonNtoaImpl<T>(*this);
    M(UInt8);
    M(Int8);
    M(UInt16);
    M(Int16);
    M(UInt32);
    M(Int32);
    M(UInt64);
    M(Int64);
#undef M
}
CATCH

TEST_F(TestInetAtonNtoa, InetNtoaReversible)
try
{
    const String aton = "tiDBIPv4StringToNum";
    const String ntoa = "IPv4NumToString";

    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<UInt32> dist;

    InferredDataVector<Nullable<UInt32>> num_vec;
    for (size_t i = 0; i < 512; ++i)
    {
        num_vec.emplace_back(dist(mt));
    }

    auto num_data_type = makeDataType<Nullable<UInt32>>();
    ColumnWithTypeAndName num_column(makeColumn<Nullable<UInt32>>(num_data_type, num_vec), num_data_type, "num");
    auto str_column = executeFunction(ntoa, num_column);
    auto num_column_2 = executeFunction(aton, str_column);
    ASSERT_COLUMN_EQ(num_column, num_column_2);
}
CATCH

TEST_F(TestInetAtonNtoa, Inet6Aton)
try
{
    const String func_name = "tiDBIPv6StringToNum";

    // empty column
    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>({}), executeFunction(func_name, createColumn<String>({})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({}),
        executeFunction(func_name, createColumn<Nullable<String>>({})));

    // const null-only column
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(1, {}),
        executeFunction(func_name, createConstColumn<Nullable<String>>(1, {})));

    // const non-null ipv4
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(1, toBinary(0x0000'0001)),
        executeFunction(func_name, createConstColumn<Nullable<String>>(1, "0.0.0.1")));

    // const non-null ipv6
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(1, toBinary({{0xFDFE'0000, 0x0000'0000, 0x5A55'CAFF, 0xFEFA'9089}})),
        executeFunction(func_name, createConstColumn<Nullable<String>>(1, "fdfe::5a55:caff:fefa:9089")));

    // valid ipv4
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>(toBinariesV4(
            {0x0102'0304, 0x0001'0001, 0x0100'0100, 0x6F00'150C, 0x0001'0203, 0x0000'0000, 0x00FF'00FF, 0xFFFF'FFFF})),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>(
                {"1.2.3.4",
                 "0.1.0.1",
                 "1.0.1.0",
                 "111.0.21.012",
                 "0000.1.2.3",
                 "00.000.0000.00000",
                 "0.255.0.255",
                 "255.255.255.255"})));

    // invalid ipv4
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>(DataVectorString(21)),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>(
                {"",          "1.2",      "1.2.3",      "1.0.1.0a",  "a0.1.2.3",  "255",       "255.",
                 "....255",   "...255",   "...255.255", "..255",     "..255.255", ".255",      ".255...255",
                 ".255..255", ".255.255", ".255.255.",  "1.2.3.256", "1.256.3.4", "1.2.256.4", "256.2.3.4"})));

    // valid ipv6

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>(toBinariesV6({
            {0x0001'0002, 0x0003'0004, 0x0005'0006, 0x0007'0008},
            {0x0001'0002, 0x0003'0004, 0x0005'0006, 0x0000'0007},
            {0x0001'0002, 0x0003'0004, 0x0005'0000, 0x0000'0000},
            {0x0001'0002, 0x0003'0004, 0x0005'0000, 0x0000'0007},
            {0x0000'0000, 0x0000'0000, 0x0000'0000, 0x0000'0000},
            {0xFDFE'0000, 0x0000'0000, 0x5A55'CAFF, 0xFEFA'9089},
            {0xFDFE'0000, 0x0000'0000, 0x5A55'CAFF, 0xFEFA'9089},
            {0x00FF'00FF, 0x00FF'00FF, 0x00FF'00FF, 0x00FF'00FF},
        })),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>(
                {"1:2:3:4:5:6:7:8",
                 "1:2:3:4:5:6::7",
                 "1:2:3:4:5::",
                 "1:2:3:4:5::7",
                 "::",
                 "fdfe::5a55:caff:fefa:9089",
                 "FDFE::5A55:CAFF:FEFA:9089",
                 "ff:ff:ff:ff:ff:ff:ff:ff"})));

    // valid ipv4-mapped ipv6
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>(toBinariesV6({
            {0x0000'0000, 0x0000'0000, 0x0000'FFFF, 0xA9DB'0D85},
            {0x0000'0000, 0x0000'0000, 0x0000'FFFF, 0x0101'0101},
            {0x0000'0000, 0x0000'0000, 0x0000'0000, 0x0101'0101},
        })),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({"::FFFF:169.219.13.133", "::FFFF:1.1.1.1", "::1.1.1.1"})));

    // invalid ipv4-mapped ipv6
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({{}, {}}),
        executeFunction(func_name, createColumn<Nullable<String>>({"::FFFF:169.256.13.133", "::1.1.1.x"})));
}
CATCH

TEST_F(TestInetAtonNtoa, Inet6Ntoa)
try
{
    const String func_name = "tiDBIPv6NumToString";

    // empty column
    ASSERT_COLUMN_EQ(createColumn<Nullable<String>>({}), executeFunction(func_name, createColumn<String>({})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({}),
        executeFunction(func_name, createColumn<Nullable<String>>({})));

    // const null-only column
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(1, {}),
        executeFunction(func_name, createConstColumn<Nullable<String>>(1, {})));

    // const non-null ipv4
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(1, "0.0.0.1"),
        executeFunction(func_name, createConstColumn<Nullable<String>>(1, toBinary(0x0000'0001))));

    // const non-null ipv6
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(1, "fdfe::5a55:caff:fefa:9089"),
        executeFunction(
            func_name,
            createConstColumn<Nullable<String>>(1, toBinary({{0xFDFE'0000, 0x0000'0000, 0x5A55'CAFF, 0xFEFA'9089}}))));

    // valid ipv4
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>(
            {"1.2.3.4", "0.1.0.1", "1.0.1.0", "111.0.21.12", "0.1.2.3", "0.0.0.0", "0.255.0.255", "255.255.255.255"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>(toBinariesV4(
                {0x0102'0304,
                 0x0001'0001,
                 0x0100'0100,
                 0x6F00'150C,
                 0x0001'0203,
                 0x0000'0000,
                 0x00FF'00FF,
                 0xFFFF'FFFF}))));

    // valid ipv6
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>(
            {"1:2:3:4:5:6:7:8",
             "1:2:3:4:5:6:0:7",
             "1:2:3:4:5::",
             "1:2:3:4:5::7",
             "::",
             "fdfe::5a55:caff:fefa:9089",
             "ff:ff:ff:ff:ff:ff:ff:ff"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>(toBinariesV6({
                {0x0001'0002, 0x0003'0004, 0x0005'0006, 0x0007'0008},
                {0x0001'0002, 0x0003'0004, 0x0005'0006, 0x0000'0007},
                {0x0001'0002, 0x0003'0004, 0x0005'0000, 0x0000'0000},
                {0x0001'0002, 0x0003'0004, 0x0005'0000, 0x0000'0007},
                {0x0000'0000, 0x0000'0000, 0x0000'0000, 0x0000'0000},
                {0xFDFE'0000, 0x0000'0000, 0x5A55'CAFF, 0xFEFA'9089},
                {0x00FF'00FF, 0x00FF'00FF, 0x00FF'00FF, 0x00FF'00FF},
            }))));

    // valid ipv4-mapped ipv6
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"::ffff:169.219.13.133", "::ffff:1.1.1.1", "::1.1.1.1"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>(toBinariesV6({
                {0x0000'0000, 0x0000'0000, 0x0000'FFFF, 0xA9DB'0D85},
                {0x0000'0000, 0x0000'0000, 0x0000'FFFF, 0x0101'0101},
                {0x0000'0000, 0x0000'0000, 0x0000'0000, 0x0101'0101},
            }))));

    // invalid cases: wrong length
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>(DataVectorString{{}, {}, {}}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>(toBinariesV6({
                {0x0001'0002, 0x0003'0004},
                {0x0001'0002, 0x0003'0004, 0x0005'0006},
                {0x0001'0002, 0x0003'0004, 0x0005'0000, 0x0000'0000, 0x0000'0000},
            }))));
}
CATCH

TEST_F(TestInetAtonNtoa, Inet6NtoaReversible)
try
{
    const String aton = "tiDBIPv6StringToNum";
    const String ntoa = "tiDBIPv6NumToString";

    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<DB::UInt32> dist;

    std::vector<std::vector<UInt32>> num_vec;
    // ipv4
    for (size_t i = 0; i < 10000; ++i)
    {
        std::vector<UInt32> v = {dist(mt)};
        num_vec.emplace_back(std::move(v));
    }
    // ipv6
    for (size_t i = 0; i < 10000; ++i)
    {
        std::vector<UInt32> v;
        for (size_t j = 0; j < 4; ++j)
            v.emplace_back(dist(mt));
        num_vec.emplace_back(std::move(v));
    }
    // ipv4-mapped ipv6
    for (size_t i = 0; i < 10000; ++i)
    {
        std::vector<UInt32> v = {0, 0, 0x0000'FFFF, dist(mt)};
        num_vec.emplace_back(std::move(v));
    }

    auto bin_vec = toBinariesV6(num_vec);
    auto bin_data_type = makeDataType<Nullable<String>>();
    ColumnWithTypeAndName bin_column(makeColumn<Nullable<String>>(bin_data_type, bin_vec), bin_data_type, "bin");
    auto str_column = executeFunction(ntoa, bin_column);
    auto bin_column_2 = executeFunction(aton, str_column);
    ASSERT_COLUMN_EQ(bin_column, bin_column_2);
}
CATCH

} // namespace tests
} // namespace DB
