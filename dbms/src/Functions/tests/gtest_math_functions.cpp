// Copyright 2022 PingCAP, Ltd.
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

#include <Functions/MathVectorization/Switch.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <mutex>
#include <random>

namespace DB
{
namespace tests
{

static constexpr size_t ROW_NUMBER = 200000;
static constexpr double ERROR_LIMIT = std::numeric_limits<double>::epsilon();

static inline std::mutex vectorization_mutex;

struct VectorizationGardian
{
    std::unique_lock<std::mutex> holder;
    VectorizationGardian()
        : holder(vectorization_mutex)
    {
#ifdef TIFLASH_HAS_MATH_VECTORIZATION_SUPPORT
        MathVectorization::enableVectorization();
#endif
    }
    ~VectorizationGardian()
    {
#ifdef TIFLASH_HAS_MATH_VECTORIZATION_SUPPORT
        MathVectorization::disableVectorization();
#endif
    }
};

class TestFunctionMath : public DB::tests::FunctionTest
{
public:
    uint64_t seed;
    ColumnWithTypeAndName column;
    ColumnWithTypeAndName tangent;
    TestFunctionMath()
    {
        std::vector<double> fields(ROW_NUMBER);
        std::random_device dev{};
        seed = dev();
        {
            auto engine = std::default_random_engine{seed};
            auto dist = std::uniform_real_distribution<double>{
                -10000,
                +10000};
            for (auto & i : fields)
            {
                i = dist(engine);
            }
            column = createColumn<double>(fields);
        }
        {
            auto engine = std::default_random_engine{seed};
            auto dist = std::uniform_real_distribution<double>{
                -1.45,
                +1.45};
            for (auto & i : fields)
            {
                i = dist(engine);
            }
            tangent = createColumn<double>(fields);
        }
    }
};

#define MATH_TEST_INSTANCE(NAME, FUNC, SCALE, DATA_SET, GUARDIAN)                      \
    TEST_F(TestFunctionMath, NAME)                                                     \
    try                                                                                \
    {                                                                                  \
        GUARDIAN;                                                                      \
        auto result = executeFunction(#FUNC, DATA_SET);                                \
        for (size_t i = 0; i < ROW_NUMBER; ++i)                                        \
        {                                                                              \
            Field a, b{};                                                              \
            result.column->get(i, a);                                                  \
            DATA_SET.column->get(i, b);                                                \
            auto x = a.get<double>();                                                  \
            auto y = ::FUNC(b.get<double>());                                          \
            EXPECT_EQ(std::isnormal(x), std::isnormal(y));                             \
            EXPECT_EQ(std::isnan(x), std::isnan(y));                                   \
            EXPECT_EQ(std::isinf(x), std::isinf(y));                                   \
            EXPECT_EQ(std::isfinite(x), std::isfinite(y));                             \
            if (std::isnormal(x) && std::isnormal(y))                                  \
                EXPECT_LE(std::abs(x - y), ERROR_LIMIT * SCALE) << ", seed: " << seed; \
        }                                                                              \
    }                                                                                  \
    CATCH

MATH_TEST_INSTANCE(VectorizedSin, sin, 10, column, VectorizationGardian gardian{});
MATH_TEST_INSTANCE(VectorizedCos, cos, 10, column, VectorizationGardian gardian{});
MATH_TEST_INSTANCE(VectorizedTan, tan, 10, tangent, VectorizationGardian gardian{});
MATH_TEST_INSTANCE(VectorizedAsin, asin, 10, column, VectorizationGardian gardian{});
MATH_TEST_INSTANCE(VectorizedAcos, acos, 10, column, VectorizationGardian gardian{});
MATH_TEST_INSTANCE(VectorizedAtan, atan, 10, column, VectorizationGardian gardian{});
MATH_TEST_INSTANCE(VectorizedLog, log, 10, column, VectorizationGardian gardian{});
MATH_TEST_INSTANCE(VectorizedLog2, log2, 10, column, VectorizationGardian gardian{});
MATH_TEST_INSTANCE(VectorizedLog10, log10, 10, column, VectorizationGardian gardian{});
MATH_TEST_INSTANCE(VectorizedExp, exp, 10, column, VectorizationGardian gardian{});

MATH_TEST_INSTANCE(ScalarSin, sin, 1, column, std::unique_lock<std::mutex> gardian{vectorization_mutex});
MATH_TEST_INSTANCE(ScalarCos, cos, 1, column, std::unique_lock<std::mutex> gardian{vectorization_mutex});
MATH_TEST_INSTANCE(ScalarTan, tan, 1, column, std::unique_lock<std::mutex> gardian{vectorization_mutex});
MATH_TEST_INSTANCE(ScalarAsin, asin, 1, column, std::unique_lock<std::mutex> gardian{vectorization_mutex});
MATH_TEST_INSTANCE(ScalarAcos, acos, 1, column, std::unique_lock<std::mutex> gardian{vectorization_mutex});
MATH_TEST_INSTANCE(ScalarAtan, atan, 1, column, std::unique_lock<std::mutex> gardian{vectorization_mutex});
MATH_TEST_INSTANCE(ScalarLog, log, 1, column, std::unique_lock<std::mutex> gardian{vectorization_mutex});
MATH_TEST_INSTANCE(ScalarLog2, log2, 1, column, std::unique_lock<std::mutex> gardian{vectorization_mutex});
MATH_TEST_INSTANCE(ScalarLog10, log10, 1, column, std::unique_lock<std::mutex> gardian{vectorization_mutex});
MATH_TEST_INSTANCE(ScalarExp, exp, 1, column, std::unique_lock<std::mutex> gardian{vectorization_mutex});

} // namespace tests
} // namespace DB
