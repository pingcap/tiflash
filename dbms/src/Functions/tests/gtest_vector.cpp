// Copyright 2024 PingCAP, Inc.
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

#include <Core/Field.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <optional>

namespace DB
{
namespace tests
{
class Vector : public DB::tests::FunctionTest
{
};

TEST_F(Vector, Dims)
try
{
    // Fn(Column)
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt32>>({0, 2, 3, std::nullopt}),
        executeFunction(
            "vecDims",
            createColumn<Nullable<Array>>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{}, Array{1.0, 2.0}, Array{1.0, 2.0, 3.0}, std::nullopt})));

    // Fn(Column)
    ASSERT_COLUMN_EQ(
        createColumn<UInt32>({0, 2, 3}),
        executeFunction(
            "vecDims",
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{}, Array{1.0, 2.0}, Array{1.0, 2.0, 3.0}})));

    // Fn(Const)
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt32>(3, 2),
        executeFunction(
            "vecDims",
            createConstColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                3, //
                Array{1.0, 2.0})));
}
CATCH

TEST_F(Vector, L2Norm)
try
{
    // Fn(Column)
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({0.0, 5.0, 1.0}),
        executeFunction(
            "vecL2Norm",
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{}, Array{3.0, 4.0}, Array{0.0, 1.0}})));

    // Fn(Column)
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({0.0, 5.0, 1.0, std::nullopt}),
        executeFunction(
            "vecL2Norm",
            createColumn<Nullable<Array>>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{}, Array{3.0, 4.0}, Array{0.0, 1.0}, std::nullopt})));

    // Fn(Const)
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Float64>>(3, 5.0),
        executeFunction(
            "vecL2Norm",
            createConstColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                3, //
                Array{3.0, 4.0})));
}
CATCH

TEST_F(Vector, L2Distance)
try
{
    // Fn(NullableColumn, Column)
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({5.0, 1.0, INFINITY, std::nullopt}),
        executeFunction(
            "vecL2Distance",
            createColumn<Nullable<Array>>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{0.0, 0.0}, Array{0.0, 0.0}, Array{3e38}, std::nullopt}),
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{3.0, 4.0}, Array{0.0, 1.0}, Array{-3e38}, Array{1}})));

    // Fn(Column, Column)
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({5.0, 1.0, INFINITY}),
        executeFunction(
            "vecL2Distance",
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{0.0, 0.0}, Array{0.0, 0.0}, Array{3e38}}),
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{3.0, 4.0}, Array{0.0, 1.0}, Array{-3e38}})));

    ASSERT_THROW(
        executeFunction(
            "vecL2Distance",
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{1.0, 2.0}}),
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{3.0}})),
        Exception);

    // Fn(Const, Const)
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Float64>>(3, 5.0),
        executeFunction(
            "vecL2Distance",
            createConstColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                3,
                Array{0.0, 0.0}),
            createConstColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                3,
                Array{3.0, 4.0})));

    // Fn(Const, Column)
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({5.0, 1.0, 1.0}),
        executeFunction(
            "vecL2Distance",
            createConstColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                3,
                Array{0.0, 0.0}),
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{3.0, 4.0}, Array{0.0, 1.0}, Array{0.0, 1.0}})));
}
CATCH

TEST_F(Vector, NegativeInnerProduct)
try
{
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({-11.0, -INFINITY}),
        executeFunction(
            "vecNegativeInnerProduct",
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{1.0, 2.0}, Array{3e38}}),
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{3.0, 4.0}, Array{3e38}})));

    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Float64>>(3, -11.0),
        executeFunction(
            "vecNegativeInnerProduct",
            createConstColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                3,
                Array{1.0, 2.0}),
            createConstColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                3,
                Array{3.0, 4.0})));

    ASSERT_THROW(
        executeFunction(
            "vecNegativeInnerProduct",
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{1.0, 2.0}}),
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{3.0}})),
        Exception);
}
CATCH

TEST_F(Vector, CosineDistance)
try
{
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>(
            {0.0,
             1.0, // CosDistance to (0,0) cannot be calculated, clapped to 1.0
             0.0,
             1.0,
             2.0,
             0.0,
             2.0,
             std::nullopt}),
        executeFunction(
            "tidbRoundWithFrac",
            executeFunction(
                "vecCosineDistance",
                createColumn<Array>(
                    std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                    {Array{1.0, 2.0},
                     Array{1.0, 2.0},
                     Array{1.0, 1.0},
                     Array{1.0, 0.0},
                     Array{1.0, 1.0},
                     Array{1.0, 1.0},
                     Array{1.0, 1.0},
                     Array{3e38}}),
                createColumn<Array>(
                    std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                    {Array{2.0, 4.0},
                     Array{0.0, 0.0},
                     Array{1.0, 1.0},
                     Array{0.0, 2.0},
                     Array{-1.0, -1.0},
                     Array{1.1, 1.1},
                     Array{-1.1, -1.1},
                     Array{3e38}})),
            createConstColumn<int>(8, 1)));

    ASSERT_THROW(
        executeFunction(
            "vecCosineDistance",
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{1.0, 2.0}}),
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{3.0}})),
        Exception);
}
CATCH

TEST_F(Vector, L1Distance)
try
{
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Float64>>({7.0, 1.0, INFINITY}),
        executeFunction(
            "vecL1Distance",
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{0.0, 0.0}, Array{0.0, 0.0}, Array{3e38}}),
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{3.0, 4.0}, Array{0.0, 1.0}, Array{-3e38}})));

    ASSERT_THROW(
        executeFunction(
            "vecL1Distance",
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{1.0, 2.0}}),
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{3.0}})),
        Exception);
}
CATCH

TEST_F(Vector, IsNull)
try
{
    ASSERT_COLUMN_EQ(
        createColumn<UInt8>({0, 1}),
        executeFunction(
            "isNull",
            createColumn<Nullable<Array>>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{1.0, 2.0}, std::nullopt})));
}
CATCH

TEST_F(Vector, CastAsString)
try
{
    ASSERT_COLUMN_EQ(
        createColumn<String>({"[]", "[1,2]"}),
        executeFunction(
            "cast_vector_float32_as_string",
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{}, Array{1.0, 2.0}})));
}
CATCH

TEST_F(Vector, CastAsVector)
try
{
    ASSERT_COLUMN_EQ(
        createColumn<Array>(
            std::make_tuple(std::make_shared<DataTypeFloat32>()), //
            {Array{}, Array{1.0, 2.0}}),
        executeFunction(
            "cast_vector_float32_as_vector_float32",
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{}, Array{1.0, 2.0}})));
}
CATCH

TEST_F(Vector, Compare)
try
{
    ASSERT_COLUMN_EQ(
        createColumn<UInt8>({0, 1, 0, 1, 0, 1}),
        executeFunction(
            "less",
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{1.0, 2.0}, Array{1.0, 2.0}, Array{1.0, 2.0}, Array{1.0, 1.0}, Array{1.0, 2.0}, Array{}}),
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{1.0, 2.0},
                 Array{2.0, 4.0},
                 Array{0.0, 1.0},
                 Array{1.0, 1.0, 1.0},
                 Array{0.0, 2.0, 3.0},
                 Array{0.0}})));

    ASSERT_COLUMN_EQ(
        createColumn<UInt8>({0, 0, 1, 0, 1, 0}),
        executeFunction(
            "greater",
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{1.0, 2.0}, Array{1.0, 2.0}, Array{1.0, 2.0}, Array{1.0, 1.0}, Array{1.0, 2.0}, Array{}}),
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{1.0, 2.0},
                 Array{2.0, 4.0},
                 Array{0.0, 1.0},
                 Array{1.0, 1.0, 1.0},
                 Array{0.0, 2.0, 3.0},
                 Array{0.0}})));

    // equals
    ASSERT_COLUMN_EQ(
        createColumn<UInt8>({1, 0, 1, 0}),
        executeFunction(
            "equals",
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{1.0, 2.0}, Array{1.0, 2.0}, Array{}, Array{}}),
            createColumn<Array>(
                std::make_tuple(std::make_shared<DataTypeFloat32>()), //
                {Array{1.0, 2.0}, Array{2.0, 4.0}, Array{}, Array{1.0, 1.0, 1.0}})));
}
CATCH

} // namespace tests
} // namespace DB
