// Copyright 2026 PingCAP, Inc.
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

#include <Columns/ColumnsNumber.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <samefn.h>

#include <bit>
#include <numbers>
#include <vector>

/// SIN is computed by samefn (samefn.h), the correctly rounded sin shared with TiDB and TiKV.
/// These tests check that TiFlash calls it: one batch call for a Float64 column, and a column
/// of another type converted to Float64 first.

namespace DB::tests
{
class TestFunctionSinSamefn : public DB::tests::FunctionTest
{
protected:
    const PaddedPODArray<Float64> & result(const ColumnWithTypeAndName & res)
    {
        return typeid_cast<const ColumnFloat64 &>(*res.column).getData();
    }
};

TEST_F(TestFunctionSinSamefn, Float64Column)
{
    constexpr auto pi = std::numbers::pi;
    const std::vector<Float64> xs = {0.0, -0.0, 1.0, -1.0, 0.5, 100.0, 1e22, 1e300, 4.9e-324, pi, -pi, 2 * pi};
    auto res = executeFunction("sin", createColumn<Float64>(xs));
    const auto & out = result(res);
    ASSERT_EQ(out.size(), xs.size());
    for (size_t i = 0; i < xs.size(); ++i)
        ASSERT_EQ(std::bit_cast<UInt64>(samefn_sin_v1(xs[i])), std::bit_cast<UInt64>(out[i])) << "sin(" << xs[i] << ")";
    // sin(pi) correctly rounded; a platform libm may differ in the last bits.
    ASSERT_EQ(std::bit_cast<UInt64>(out[9]), 0x3ca1a62633145c07ULL);
}

TEST_F(TestFunctionSinSamefn, Int64Column)
{
    const std::vector<Int64> xs = {0, 1, -1, 2, 3, 100, -100};
    auto res = executeFunction("sin", createColumn<Int64>(xs));
    const auto & out = result(res);
    ASSERT_EQ(out.size(), xs.size());
    for (size_t i = 0; i < xs.size(); ++i)
    {
        const auto x = static_cast<Float64>(xs[i]);
        ASSERT_EQ(std::bit_cast<UInt64>(samefn_sin_v1(x)), std::bit_cast<UInt64>(out[i])) << "sin(" << xs[i] << ")";
    }
}
} // namespace DB::tests
