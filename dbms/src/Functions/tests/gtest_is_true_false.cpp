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

#include <TestUtils/FunctionTestUtils.h>

namespace DB::tests
{
namespace
{
class IsTrueFalseTest : public FunctionTest
{
protected:
    void testOnlyNull(const String & func, bool with_null)
    {
        auto expected = createConstColumn<Int64>(5, 0);
        if (with_null)
            expected = createConstColumn<Nullable<Int64>>(5, std::nullopt);

        ASSERT_COLUMN_EQ(expected, executeFunction(func, createOnlyNullColumn(5)));

        ASSERT_COLUMN_EQ(expected, executeFunction(func, createOnlyNullColumnConst(5)));
    }
};

TEST_F(IsTrueFalseTest, isTrue_testOnlyNull)
try
{
    testOnlyNull("isTrue", false);
    testOnlyNull("isTrueWithNull", true);
}
CATCH

TEST_F(IsTrueFalseTest, isFalse_testOnlyNull)
try
{
    testOnlyNull("isFalse", false);
    testOnlyNull("isFalseWithNull", true);
}
CATCH

#define APPLY_FOR_INT_FLOAT_TYPES(M) \
    M(Int8)                          \
    M(UInt8)                         \
    M(Int16)                         \
    M(UInt16)                        \
    M(Int32)                         \
    M(UInt32)                        \
    M(Int64)                         \
    M(UInt64)                        \
    M(Float32)                       \
    M(Float64)

#define TEST_IS_TRUE_NON_NULLABLE(TYPE)                                                                             \
    TEST_F(IsTrueFalseTest, isTrue_##TYPE##_NonNullable)                                                            \
    try                                                                                                             \
    {                                                                                                               \
        ASSERT_COLUMN_EQ(                                                                                           \
            createColumn<Int64>({0, 1, 1}),                                                                         \
            executeFunction("isTrue", createColumn<TYPE>({0, 1, static_cast<TYPE>(-1)})));                          \
        ASSERT_COLUMN_EQ(createConstColumn<Int64>(5, 0), executeFunction("isTrue", createConstColumn<TYPE>(5, 0))); \
        ASSERT_COLUMN_EQ(createConstColumn<Int64>(5, 1), executeFunction("isTrue", createConstColumn<TYPE>(5, 2))); \
    }                                                                                                               \
    CATCH

#define TEST_IS_TRUE_NULLABLE(TYPE)                                                                                \
    TEST_F(IsTrueFalseTest, isTrue_##TYPE##_Nullable)                                                              \
    try                                                                                                            \
    {                                                                                                              \
        ASSERT_COLUMN_EQ(                                                                                          \
            createColumn<Int64>({0, 1, 1, 0}),                                                                     \
            executeFunction("isTrue", createColumn<Nullable<TYPE>>({0, 1, static_cast<TYPE>(-1), std::nullopt}))); \
        ASSERT_COLUMN_EQ(                                                                                          \
            createConstColumn<Int64>(5, 0),                                                                        \
            executeFunction("isTrue", createConstColumn<Nullable<TYPE>>(5, 0)));                                   \
        ASSERT_COLUMN_EQ(                                                                                          \
            createConstColumn<Int64>(5, 1),                                                                        \
            executeFunction("isTrue", createConstColumn<Nullable<TYPE>>(5, 2)));                                   \
        ASSERT_COLUMN_EQ(                                                                                          \
            createConstColumn<Int64>(5, 0),                                                                        \
            executeFunction("isTrue", createConstColumn<Nullable<TYPE>>(5, std::nullopt)));                        \
    }                                                                                                              \
    CATCH

#define TEST_IS_TRUE_WITH_NULL_NON_NULLABLE(TYPE)                                                  \
    TEST_F(IsTrueFalseTest, isTrueWithNull_##TYPE##_NonNullable)                                   \
    try                                                                                            \
    {                                                                                              \
        ASSERT_COLUMN_EQ(                                                                          \
            createColumn<Int64>({0, 1, 1}),                                                        \
            executeFunction("isTrueWithNull", createColumn<TYPE>({0, 1, static_cast<TYPE>(-1)}))); \
        ASSERT_COLUMN_EQ(                                                                          \
            createConstColumn<Int64>(5, 0),                                                        \
            executeFunction("isTrueWithNull", createConstColumn<TYPE>(5, 0)));                     \
        ASSERT_COLUMN_EQ(                                                                          \
            createConstColumn<Int64>(5, 1),                                                        \
            executeFunction("isTrueWithNull", createConstColumn<TYPE>(5, 2)));                     \
    }                                                                                              \
    CATCH

#define TEST_IS_TRUE_WITH_NULL_NULLABLE(TYPE)                                                       \
    TEST_F(IsTrueFalseTest, isTrueWithNull_##TYPE##_Nullable)                                       \
    try                                                                                             \
    {                                                                                               \
        ASSERT_COLUMN_EQ(                                                                           \
            createColumn<Nullable<Int64>>({0, 1, 1, std::nullopt}),                                 \
            executeFunction(                                                                        \
                "isTrueWithNull",                                                                   \
                createColumn<Nullable<TYPE>>({0, 1, static_cast<TYPE>(-1), std::nullopt})));        \
        ASSERT_COLUMN_EQ(                                                                           \
            createConstColumn<Int64>(5, 0),                                                         \
            executeFunction("isTrueWithNull", createConstColumn<Nullable<TYPE>>(5, 0)));            \
        ASSERT_COLUMN_EQ(                                                                           \
            createConstColumn<Int64>(5, 1),                                                         \
            executeFunction("isTrueWithNull", createConstColumn<Nullable<TYPE>>(5, 2)));            \
        ASSERT_COLUMN_EQ(                                                                           \
            createConstColumn<Nullable<Int64>>(5, std::nullopt),                                    \
            executeFunction("isTrueWithNull", createConstColumn<Nullable<TYPE>>(5, std::nullopt))); \
    }                                                                                               \
    CATCH

#define TEST_IS_FALSE_NON_NULLABLE(TYPE)                                                                             \
    TEST_F(IsTrueFalseTest, isFalse_##TYPE##_NonNullable)                                                            \
    try                                                                                                              \
    {                                                                                                                \
        ASSERT_COLUMN_EQ(                                                                                            \
            createColumn<Int64>({1, 0, 0}),                                                                          \
            executeFunction("isFalse", createColumn<TYPE>({0, 1, static_cast<TYPE>(-1)})));                          \
        ASSERT_COLUMN_EQ(createConstColumn<Int64>(5, 1), executeFunction("isFalse", createConstColumn<TYPE>(5, 0))); \
        ASSERT_COLUMN_EQ(createConstColumn<Int64>(5, 0), executeFunction("isFalse", createConstColumn<TYPE>(5, 2))); \
    }                                                                                                                \
    CATCH

#define TEST_IS_FALSE_NULLABLE(TYPE)                                                                                \
    TEST_F(IsTrueFalseTest, isFalse_##TYPE##_Nullable)                                                              \
    try                                                                                                             \
    {                                                                                                               \
        ASSERT_COLUMN_EQ(                                                                                           \
            createColumn<Int64>({1, 0, 0, 0}),                                                                      \
            executeFunction("isFalse", createColumn<Nullable<TYPE>>({0, 1, static_cast<TYPE>(-1), std::nullopt}))); \
        ASSERT_COLUMN_EQ(                                                                                           \
            createConstColumn<Int64>(5, 1),                                                                         \
            executeFunction("isFalse", createConstColumn<Nullable<TYPE>>(5, 0)));                                   \
        ASSERT_COLUMN_EQ(                                                                                           \
            createConstColumn<Int64>(5, 0),                                                                         \
            executeFunction("isFalse", createConstColumn<Nullable<TYPE>>(5, 2)));                                   \
        ASSERT_COLUMN_EQ(                                                                                           \
            createConstColumn<Int64>(5, 0),                                                                         \
            executeFunction("isFalse", createConstColumn<Nullable<TYPE>>(5, std::nullopt)));                        \
    }                                                                                                               \
    CATCH

#define TEST_IS_FALSE_WITH_NULL_NON_NULLABLE(TYPE)                                                  \
    TEST_F(IsTrueFalseTest, isFalseWithNull_##TYPE##_NonNullable)                                   \
    try                                                                                             \
    {                                                                                               \
        ASSERT_COLUMN_EQ(                                                                           \
            createColumn<Int64>({1, 0, 0}),                                                         \
            executeFunction("isFalseWithNull", createColumn<TYPE>({0, 1, static_cast<TYPE>(-1)}))); \
        ASSERT_COLUMN_EQ(                                                                           \
            createConstColumn<Int64>(5, 1),                                                         \
            executeFunction("isFalseWithNull", createConstColumn<TYPE>(5, 0)));                     \
        ASSERT_COLUMN_EQ(                                                                           \
            createConstColumn<Int64>(5, 0),                                                         \
            executeFunction("isFalseWithNull", createConstColumn<TYPE>(5, 2)));                     \
    }                                                                                               \
    CATCH

#define TEST_IS_FALSE_WITH_NULL_NULLABLE(TYPE)                                                       \
    TEST_F(IsTrueFalseTest, isFalseWithNull_##TYPE##_Nullable)                                       \
    try                                                                                              \
    {                                                                                                \
        ASSERT_COLUMN_EQ(                                                                            \
            createColumn<Nullable<Int64>>({1, 0, 0, std::nullopt}),                                  \
            executeFunction(                                                                         \
                "isFalseWithNull",                                                                   \
                createColumn<Nullable<TYPE>>({0, 1, static_cast<TYPE>(-1), std::nullopt})));         \
        ASSERT_COLUMN_EQ(                                                                            \
            createConstColumn<Int64>(5, 1),                                                          \
            executeFunction("isFalseWithNull", createConstColumn<Nullable<TYPE>>(5, 0)));            \
        ASSERT_COLUMN_EQ(                                                                            \
            createConstColumn<Int64>(5, 0),                                                          \
            executeFunction("isFalseWithNull", createConstColumn<Nullable<TYPE>>(5, 2)));            \
        ASSERT_COLUMN_EQ(                                                                            \
            createConstColumn<Nullable<Int64>>(5, std::nullopt),                                     \
            executeFunction("isFalseWithNull", createConstColumn<Nullable<TYPE>>(5, std::nullopt))); \
    }                                                                                                \
    CATCH

APPLY_FOR_INT_FLOAT_TYPES(TEST_IS_TRUE_NON_NULLABLE)
APPLY_FOR_INT_FLOAT_TYPES(TEST_IS_TRUE_NULLABLE)
APPLY_FOR_INT_FLOAT_TYPES(TEST_IS_TRUE_WITH_NULL_NON_NULLABLE)
APPLY_FOR_INT_FLOAT_TYPES(TEST_IS_TRUE_WITH_NULL_NULLABLE)

APPLY_FOR_INT_FLOAT_TYPES(TEST_IS_FALSE_NON_NULLABLE)
APPLY_FOR_INT_FLOAT_TYPES(TEST_IS_FALSE_NULLABLE)
APPLY_FOR_INT_FLOAT_TYPES(TEST_IS_FALSE_WITH_NULL_NON_NULLABLE)
APPLY_FOR_INT_FLOAT_TYPES(TEST_IS_FALSE_WITH_NULL_NULLABLE)

#define APPLY_FOR_DECIMAL_TYPES(M) \
    M(Decimal32)                   \
    M(Decimal64)                   \
    M(Decimal128)                  \
    M(Decimal256)

TEST_F(IsTrueFalseTest, isTrueFalse_Decimal)
try
{
    ColumnWithTypeAndName input;
#define M(DECIMAL)                                                                               \
    input = createColumn<DECIMAL>(                                                               \
        std::make_tuple(maxDecimalPrecision<DECIMAL>(), 0),                                      \
        {DecimalField<DECIMAL>(DECIMAL::NativeType(0), 0),                                       \
         DecimalField<DECIMAL>(DECIMAL::NativeType(1), 0),                                       \
         DecimalField<DECIMAL>(DECIMAL::NativeType(-1), 0)});                                    \
    ASSERT_COLUMN_EQ(createColumn<Int64>({0, 1, 1}), executeFunction("isTrue", input));          \
    ASSERT_COLUMN_EQ(createColumn<Int64>({0, 1, 1}), executeFunction("isTrueWithNull", input));  \
    ASSERT_COLUMN_EQ(createColumn<Int64>({1, 0, 0}), executeFunction("isFalse", input));         \
    ASSERT_COLUMN_EQ(createColumn<Int64>({1, 0, 0}), executeFunction("isFalseWithNull", input)); \
    input = createColumn<Nullable<DECIMAL>>(                                                     \
        std::make_tuple(maxDecimalPrecision<DECIMAL>(), 0),                                      \
        {DecimalField<DECIMAL>(DECIMAL::NativeType(0), 0),                                       \
         DecimalField<DECIMAL>(DECIMAL::NativeType(1), 0),                                       \
         DecimalField<DECIMAL>(DECIMAL::NativeType(-1), 0),                                      \
         std::nullopt});                                                                         \
    ASSERT_COLUMN_EQ(createColumn<Int64>({0, 1, 1, 0}), executeFunction("isTrue", input));       \
    ASSERT_COLUMN_EQ(                                                                            \
        createColumn<Nullable<Int64>>({0, 1, 1, std::nullopt}),                                  \
        executeFunction("isTrueWithNull", input));                                               \
    ASSERT_COLUMN_EQ(createColumn<Int64>({1, 0, 0, 0}), executeFunction("isFalse", input));      \
    ASSERT_COLUMN_EQ(createColumn<Nullable<Int64>>({1, 0, 0, std::nullopt}), executeFunction("isFalseWithNull", input));

    APPLY_FOR_DECIMAL_TYPES(M)
#undef M
}
CATCH

} // namespace
} // namespace DB::tests
