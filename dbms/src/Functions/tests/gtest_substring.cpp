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

#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB
{
namespace tests
{
class SubString : public DB::tests::FunctionTest
{
};

template <typename T1, typename T2>
class TestNullableSigned
{
public:
    static void operator()(SubString & sub_string)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"p.co", "ww.p", "pingcap", "com", ".com", "", "", "", {}, {}, {}}),
            sub_string.executeFunction(
                "substringUTF8",
                createColumn<Nullable<String>>(
                    {"www.pingcap.com",
                     "ww.pingcap.com",
                     "w.pingcap.com",
                     ".pingcap.com",
                     "pingcap.com",
                     "pingcap.com",
                     "pingcap.com",
                     "pingcap.com",
                     {},
                     "pingcap",
                     "pingcap"}),
                createColumn<T1>({-5, 1, 3, -3, 8, 2, -100, 0, 2, {}, -3}),
                createColumn<T2>({4, 4, 7, 4, 5, -5, 2, 3, 6, 4, {}})));
    }
};

template <typename T1, typename T2>
class TestSigned
{
public:
    static void operator()(SubString & sub_string)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"p.co", "ww.p", "pingcap", "com", ".com", "", "", "", {}}),
            sub_string.executeFunction(
                "substringUTF8",
                createColumn<Nullable<String>>(
                    {"www.pingcap.com",
                     "ww.pingcap.com",
                     "w.pingcap.com",
                     ".pingcap.com",
                     "pingcap.com",
                     "pingcap.com",
                     "pingcap.com",
                     "pingcap.com",
                     {}}),
                createColumn<T1>({-5, 1, 3, -3, 8, 2, -100, 0, 2}),
                createColumn<T2>({4, 4, 7, 4, 5, -5, 2, 3, 6})));
    }
};

template <typename T1, typename T2>
class TestNullableUnsigned
{
public:
    static void operator()(SubString & sub_string)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"p.co", "ww.p", "pingcap", "com", ".com", "", "", {}, {}, {}}),
            sub_string.executeFunction(
                "substringUTF8",
                createColumn<Nullable<String>>(
                    {"www.pingcap.com",
                     "ww.pingcap.com",
                     "w.pingcap.com",
                     ".pingcap.com",
                     "pingcap.com",
                     "pingcap.com",
                     "pingcap.com",
                     {},
                     "pingcap",
                     "pingcap"}),
                createColumn<T1>({11, 1, 3, 10, 8, 2, 0, 9, {}, 7}),
                createColumn<T2>({4, 4, 7, 4, 5, 0, 3, 6, 1, {}})));
    }
};

template <typename T1, typename T2>
class TestUnsigned
{
public:
    static void operator()(SubString & sub_string)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"p.co", "ww.p", "pingcap", "com", ".com", "", "", {}}),
            sub_string.executeFunction(
                "substringUTF8",
                createColumn<Nullable<String>>(
                    {"www.pingcap.com",
                     "ww.pingcap.com",
                     "w.pingcap.com",
                     ".pingcap.com",
                     "pingcap.com",
                     "pingcap.com",
                     "pingcap.com",
                     {}}),
                createColumn<T1>({11, 1, 3, 10, 8, 2, 0, 2}),
                createColumn<T2>({4, 4, 7, 4, 5, 0, 3, 1})));
    }
};

template <typename T1, typename T2>
class TestConstPos
{
public:
    static void operator()(SubString & sub_string)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"w", "ww", "w.p", ".pin"}),
            sub_string.executeFunction(
                "substringUTF8",
                createColumn<Nullable<String>>({"www.pingcap.com", "ww.pingcap.com", "w.pingcap.com", ".pingcap.com"}),
                createConstColumn<T1>(4, 1),
                createColumn<T2>({1, 2, 3, 4})));
    }
};

template <typename T1, typename T2>
class TestConstLength
{
public:
    static void operator()(SubString & sub_string)
    {
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({"www.", "w.pi", "ping", "ngca"}),
            sub_string.executeFunction(
                "substringUTF8",
                createColumn<Nullable<String>>({"www.pingcap.com", "ww.pingcap.com", "w.pingcap.com", ".pingcap.com"}),
                createColumn<T1>({1, 2, 3, 4}),
                createConstColumn<T1>(4, 4)));
    }
};

TEST_F(SubString, subStringUTF8Test)
try
{
    TestTypePair<TestNullableIntTypes, TestNullableIntTypes, TestNullableSigned, SubString>::run(*this);
    TestTypePair<TestAllIntTypes, TestAllIntTypes, TestSigned, SubString>::run(*this);

    TestTypePair<TestNullableIntTypes, TestNullableUIntTypes, TestNullableUnsigned, SubString>::run(*this);
    TestTypePair<TestNullableUIntTypes, TestNullableIntTypes, TestNullableUnsigned, SubString>::run(*this);
    TestTypePair<TestNullableUIntTypes, TestNullableUIntTypes, TestNullableUnsigned, SubString>::run(*this);

    TestTypePair<TestAllIntTypes, TestAllUIntTypes, TestUnsigned, SubString>::run(*this);
    TestTypePair<TestAllUIntTypes, TestAllIntTypes, TestUnsigned, SubString>::run(*this);
    TestTypePair<TestAllUIntTypes, TestAllUIntTypes, TestUnsigned, SubString>::run(*this);

    TestTypePair<TestAllIntTypes, TestAllIntTypes, TestConstPos, SubString>::run(*this);
    TestTypePair<TestAllUIntTypes, TestAllUIntTypes, TestConstPos, SubString>::run(*this);

    TestTypePair<TestAllIntTypes, TestAllIntTypes, TestConstLength, SubString>::run(*this);
    TestTypePair<TestAllUIntTypes, TestAllUIntTypes, TestConstLength, SubString>::run(*this);

    // column, const, const
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"www.", "ww.p", "w.pi", ".pin"}),
        executeFunction(
            "substringUTF8",
            createColumn<Nullable<String>>({"www.pingcap.com", "ww.pingcap.com", "w.pingcap.com", ".pingcap.com"}),
            createConstColumn<Nullable<Int64>>(4, 1),
            createConstColumn<Nullable<Int64>>(4, 4)));

    // const, const, const
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<String>>(1, "www."),
        executeFunction(
            "substringUTF8",
            createConstColumn<Nullable<String>>(1, "www.pingcap.com"),
            createConstColumn<Nullable<Int64>>(1, 1),
            createConstColumn<Nullable<Int64>>(1, 4)));
<<<<<<< HEAD
    // Test Null
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({{}, "www."}),
        executeFunction(
            "substringUTF8",
            createColumn<Nullable<String>>(
                {{}, "www.pingcap.com"}),
            createConstColumn<Nullable<Int64>>(2, 1),
            createConstColumn<Nullable<Int64>>(2, 4)));
=======
>>>>>>> 9f85bb2f16 (Support other integer types for SubstringUTF8 & RightUTF8 functions (#9507))
}
CATCH

} // namespace tests
} // namespace DB
