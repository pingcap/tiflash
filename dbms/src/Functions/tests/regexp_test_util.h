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

#include <Common/Exception.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TiDB/Collation/Collator.h>
#include <common/types.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <fmt/core.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#pragma GCC diagnostic pop

namespace DB
{
namespace tests
{
class Regexp : public FunctionTest
{
protected:
    static bool isColumnConstNull(const ColumnWithTypeAndName & column_with_type)
    {
        return column_with_type.column->isColumnConst() && column_with_type.column->isNullAt(0);
    }
    static bool isColumnConst(const ColumnWithTypeAndName & column_with_type)
    {
        return column_with_type.column->isColumnConst();
    }
    static bool isColumnConstNotNull(const ColumnWithTypeAndName & column_with_type)
    {
        return column_with_type.column->isColumnConst() && !column_with_type.column->isNullAt(0);
    }
    static bool isNullableColumnVector(const ColumnWithTypeAndName & column_with_type)
    {
        return !column_with_type.column->isColumnConst() && column_with_type.type->isNullable();
    }
    template <typename T>
    ColumnWithTypeAndName createNullableVectorColumn(
        const InferredDataVector<T> & vec,
        const std::vector<UInt8> & null_map)
    {
        using NullableType = Nullable<T>;
        InferredDataVector<NullableType> nullable_vec;
        for (size_t i = 0; i < null_map.size(); i++)
        {
            if (null_map[i])
                nullable_vec.push_back({});
            else
                nullable_vec.push_back(vec[i]);
        }
        return createColumn<NullableType>(nullable_vec);
    }
};

template <typename ResType, typename CaseType>
static std::vector<ResType> getResultVec(const std::vector<CaseType> & test_cases)
{
    std::vector<ResType> vecs;
    vecs.reserve(test_cases.size());
    for (const auto & elem : test_cases)
        vecs.push_back(elem.result);

    return vecs;
}

template <typename T>
static std::vector<String> getExprVec(const std::vector<T> & test_cases)
{
    std::vector<String> vecs;
    vecs.reserve(test_cases.size());
    for (const auto & elem : test_cases)
        vecs.push_back(elem.expression);

    return vecs;
}

template <typename T>
static std::vector<String> getPatVec(const std::vector<T> & test_cases)
{
    std::vector<String> vecs;
    vecs.reserve(test_cases.size());
    for (const auto & elem : test_cases)
        vecs.push_back(elem.pattern);

    return vecs;
}

template <typename T>
static std::vector<String> getReplVec(const std::vector<T> & test_cases)
{
    std::vector<String> vecs;
    vecs.reserve(test_cases.size());
    for (const auto & elem : test_cases)
        vecs.push_back(elem.replacement);

    return vecs;
}

template <typename T>
static std::vector<Int64> getPosVec(const std::vector<T> & test_cases)
{
    std::vector<Int64> vecs;
    vecs.reserve(test_cases.size());
    for (const auto & elem : test_cases)
        vecs.push_back(elem.position);

    return vecs;
}

template <typename T>
static std::vector<Int64> getOccurVec(const std::vector<T> & test_cases)
{
    std::vector<Int64> vecs;
    vecs.reserve(test_cases.size());
    for (const auto & elem : test_cases)
        vecs.push_back(elem.occurrence);

    return vecs;
}

template <typename T>
static std::vector<Int64> getRetOpVec(const std::vector<T> & test_cases)
{
    std::vector<Int64> vecs;
    vecs.reserve(test_cases.size());
    for (const auto & elem : test_cases)
        vecs.push_back(elem.return_option);

    return vecs;
}

template <typename T>
static std::vector<String> getMatchTypeVec(const std::vector<T> & test_cases)
{
    std::vector<String> vecs;
    vecs.reserve(test_cases.size());
    for (const auto & elem : test_cases)
        vecs.push_back(elem.match_type);

    return vecs;
}
} // namespace tests
} // namespace DB
