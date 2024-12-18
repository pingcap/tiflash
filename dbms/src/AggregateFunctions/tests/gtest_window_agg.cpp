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

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/AlignedBuffer.h>
#include <Common/Decimal.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <TestUtils/AggregationTestUtils.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <limits>

namespace DB
{
namespace tests
{
constexpr int scale = 2;
const std::vector<String>
    input_string_vec{"0", "71.94", "12.34", "-34.26", "80.02", "-84.39", "28.41", "45.32", "11.11", "-10.32"};
const std::vector<String>
    input_string_vec_aux{"0", "7194", "1234", "-3426", "8002", "-8439", "2841", "4532", "1111", "-1032"};
const std::vector<Int64> input_int_vec{1, -2, 7, 4, 0, -3, -1, 0, 0, 9, 2, 0, -4, 2, 6, -3, 5};
const std::vector<Int64> input_decimal_vec{
    std::stoi(input_string_vec_aux[0]),
    std::stoi(input_string_vec_aux[1]),
    std::stoi(input_string_vec_aux[2]),
    std::stoi(input_string_vec_aux[3]),
    std::stoi(input_string_vec_aux[4]),
    std::stoi(input_string_vec_aux[5]),
    std::stoi(input_string_vec_aux[6]),
    std::stoi(input_string_vec_aux[7])};

struct SumMocker
{
    inline static void add(Int64 & res, Int64 data) noexcept { res += data; }
    inline static void decrease(Int64 & res, Int64 data) noexcept { res -= data; }
    inline static void reset() noexcept {}
};

struct CountMocker
{
    inline static void add(Int64 & res, Int64) noexcept { res++; }
    inline static void decrease(Int64 & res, Int64) noexcept { res--; }
    inline static void reset() noexcept {}
};

class AvgMocker
{
public:
    AvgMocker()
        : sum(0)
        , count(0)
    {}

    inline void add(Int64 & res, Int64 data) noexcept
    {
        sum += data;
        count++;
        avgImpl(res);
    }

    inline void decrease(Int64 & res, Int64 data) noexcept
    {
        sum -= data;
        count--;
        avgImpl(res);
    }

    inline void reset() noexcept
    {
        sum = 0;
        count = 0;
    }

private:
    inline void avgImpl(Int64 & res) const noexcept { res = sum / count; }

    Int64 sum;
    Int64 count;
};

template <bool is_max>
class MinOrMaxMocker
{
public:
    inline void add(Int64 & res, Int64 data) noexcept
    {
        cmpAndChange(res, data);
        saved_values.push_back(data);
    }

    inline void decrease(Int64 & res, Int64) noexcept
    {
        saved_values.pop_front();
        res = is_max ? std::numeric_limits<Int64>::min() : std::numeric_limits<Int64>::max();

        // Inefficient, but it's ok in the ut
        for (auto value : saved_values)
            cmpAndChange(res, value);
    }

    inline void reset() noexcept { saved_values.clear(); }

private:
    static void inline cmpAndChange(Int64 & res, Int64 value) noexcept
    {
        if constexpr (is_max)
        {
            if (value > res)
                res = value;
        }
        else
        {
            if (value < res)
                res = value;
        }
    }

    std::deque<Int64> saved_values;
};

template <typename OpMocker>
struct TestCase
{
    TestCase(
        DataTypePtr type_,
        const std::vector<Int64> & input_vec_,
        const String & agg_name_,
        Int64 init_res_,
        int scale_)
        : type(type_)
        , input_vec(input_vec_)
        , agg_name(agg_name_)
        , init_res(init_res_)
        , scale(scale_)
    {}

    inline void addInMock(Int64 & res, Int64 row_idx) noexcept { mocker.add(res, input_vec[row_idx]); }
    inline void decreaseInMock(Int64 & res, Int64 row_idx) noexcept { mocker.decrease(res, input_vec[row_idx]); }
    inline void reset() noexcept { mocker.reset(); }

    const DataTypePtr type;
    const std::vector<Int64> input_vec;
    const String agg_name;
    Int64 init_res;
    int scale; // scale is 0 when test type is int
    OpMocker mocker;
};

class ExecutorWindowAgg : public DB::tests::AggregationTest
{
public:
    void SetUp() override { dre = std::default_random_engine(r()); }

private:
    // range: [begin, end]
    inline UInt32 rand(UInt32 begin, UInt32 end) noexcept
    {
        di.param(std::uniform_int_distribution<UInt32>::param_type{begin, end});
        return di(dre);
    }

protected:
    template <typename Op>
    void executeWindowAggTest(TestCase<Op> & test_case);

    static const IColumn * getInputColumn(const IDataType * type)
    {
        if (const auto * tmp = dynamic_cast<const DataTypeInt64 *>(type); tmp != nullptr)
            return &(*ExecutorWindowAgg::input_int_col);
        else if (const auto * tmp = dynamic_cast<const DataTypeDecimal128 *>(type); tmp != nullptr)
            return &(*ExecutorWindowAgg::input_decimal128_col);
        else if (const auto * tmp = dynamic_cast<const DataTypeDecimal256 *>(type); tmp != nullptr)
            return &(*ExecutorWindowAgg::input_decimal256_col);
        else
            throw Exception("Invalid data type");
    }

    static String getValue(const Field & field)
    {
        switch (field.getType())
        {
        case Field::Types::Which::Int64:
            return std::to_string(field.template get<Int64>());
        case Field::Types::Which::UInt64:
            return std::to_string(field.template get<UInt64>());
        case Field::Types::Which::Decimal128:
            return field.template get<DecimalField<Decimal128>>().toString();
        case Field::Types::Which::Decimal256:
            return field.template get<DecimalField<Decimal256>>().toString();
        default:
            throw Exception("Invalid data type");
        }
    }

    inline UInt32 getResetNum() noexcept { return rand(1, 10); }
    inline UInt32 getAddNum() noexcept { return rand(1, 5); }
    inline UInt32 getDecreaseNum(UInt32 max_num) noexcept { return rand(0, max_num); }
    inline UInt32 getRowIdx(UInt32 start, UInt32 end) noexcept { return rand(start, end); }
    inline UInt32 getResultNum() noexcept { return rand(1, 3); }

    std::random_device r;
    std::default_random_engine dre;
    std::uniform_int_distribution<UInt32> di;

    static const ColumnPtr input_int_col;
    static const ColumnPtr input_decimal128_col;
    static const ColumnPtr input_decimal256_col;

    static DataTypePtr type_int;
    static DataTypePtr type_decimal128;
    static DataTypePtr type_decimal256;
};

const ColumnPtr ExecutorWindowAgg::input_int_col = createColumn<Int64>(input_int_vec).column;
const ColumnPtr ExecutorWindowAgg::input_decimal128_col
    = createColumn<Decimal128>(std::make_tuple(10, scale), input_string_vec).column;
const ColumnPtr ExecutorWindowAgg::input_decimal256_col
    = createColumn<Decimal256>(std::make_tuple(30, scale), input_string_vec).column;

DataTypePtr ExecutorWindowAgg::type_int = std::make_shared<DataTypeInt64>();
DataTypePtr ExecutorWindowAgg::type_decimal128 = std::make_shared<DataTypeDecimal128>(10, scale);
DataTypePtr ExecutorWindowAgg::type_decimal256 = std::make_shared<DataTypeDecimal256>(30, scale);

template <typename Op>
void ExecutorWindowAgg::executeWindowAggTest(TestCase<Op> & test_case)
{
    Arena arena;
    auto context = TiFlashTestEnv::getContext();
    std::deque<int> added_row_idx_queue;

    added_row_idx_queue.clear();
    auto agg_func
        = AggregateFunctionFactory::instance().get(*context, test_case.agg_name, {test_case.type}, {}, 0, true);
    auto return_type = agg_func->getReturnType();
    AlignedBuffer agg_state;
    agg_state.reset(agg_func->sizeOfData(), agg_func->alignOfData());
    agg_func->create(agg_state.data());
    agg_func->prepareWindow(agg_state.data());

    std::vector<Int64> res_vec;
    res_vec.reserve(10);

    const UInt32 col_row_num = test_case.input_vec.size();

    UInt32 reset_num = getResetNum();
    auto res_col = return_type->createColumn();
    const IColumn * input_col = getInputColumn(test_case.type.get());

    // Start test
    for (UInt32 i = 0; i < reset_num; i++)
    {
        Int64 res = test_case.init_res;
        test_case.reset();
        std::cout << "----------- reset" << std::endl;
        agg_func->reset(agg_state.data());
        added_row_idx_queue.clear();

        // Generate a result
        const UInt32 res_num = getResultNum();
        for (UInt32 j = 0; j < res_num; j++)
        {
            // Start to add
            const UInt32 add_num = getAddNum();
            for (UInt32 k = 0; k < add_num; k++)
            {
                const UInt32 row_idx = getRowIdx(0, col_row_num - 1);
                added_row_idx_queue.push_back(row_idx);
                agg_func->add(agg_state.data(), &input_col, row_idx, &arena);
                test_case.addInMock(res, row_idx);
                std::cout << "add: " << test_case.input_vec[row_idx] << " res: " << res << std::endl;
            }

            if likely (!added_row_idx_queue.empty())
            {
                // Start to decrease
                const UInt32 decrease_num = getDecreaseNum(added_row_idx_queue.size() - 1);
                for (UInt32 k = 0; k < decrease_num; k++)
                {
                    const UInt32 row_idx = added_row_idx_queue.front();
                    added_row_idx_queue.pop_front();
                    agg_func->decrease(agg_state.data(), &input_col, row_idx, &arena);
                    test_case.decreaseInMock(res, row_idx);
                    std::cout << "decrease: " << test_case.input_vec[row_idx] << " res: " << res << std::endl;
                }
            }

            std::cout << "insert, res: " << res << std::endl;
            agg_func->insertResultInto(agg_state.data(), *res_col, &arena);
            res_vec.push_back(res);
        }
    }

    size_t res_num = res_vec.size();
    ASSERT_EQ(res_num, res_col->size());

    Field res_field;
    for (size_t i = 0; i < res_num; i++)
    {
        res_col->get(i, res_field);
        ASSERT_FALSE(res_field.isNull());
        std::cout << "i: " << i << std::endl;
        // No matter what type the result is, we always use decimal to convert the result to string so that it's easy to check result
        ASSERT_EQ(Decimal256(res_vec[i]).toString(test_case.scale), getValue(res_field));
    }
}

TEST_F(ExecutorWindowAgg, Sum)
try
{
    TestCase<SumMocker> int_case(ExecutorWindowAgg::type_int, input_int_vec, "sum", 0, 0);
    TestCase<SumMocker> decimal128_case(ExecutorWindowAgg::type_decimal128, input_decimal_vec, "sum", 0, scale);
    TestCase<SumMocker> decimal256_case(ExecutorWindowAgg::type_decimal256, input_decimal_vec, "sum", 0, scale);

    executeWindowAggTest(int_case);
    executeWindowAggTest(decimal128_case);
    executeWindowAggTest(decimal256_case);
}
CATCH

// TODO add count distinct
TEST_F(ExecutorWindowAgg, Count)
try
{
    TestCase<CountMocker> int_case(ExecutorWindowAgg::type_int, input_int_vec, "count", 0, 0);
    TestCase<CountMocker> decimal128_case(ExecutorWindowAgg::type_decimal128, input_decimal_vec, "count", 0, 0);
    TestCase<CountMocker> decimal256_case(ExecutorWindowAgg::type_decimal256, input_decimal_vec, "count", 0, 0);

    executeWindowAggTest(int_case);
    executeWindowAggTest(decimal128_case);
    executeWindowAggTest(decimal256_case);
}
CATCH

TEST_F(ExecutorWindowAgg, Avg)
try
{
    // TestCase<CountMocker> int_case(ExecutorWindowAgg::type_int, input_int_vec, "avg", 0, 0);
    // TestCase<CountMocker> decimal128_case(ExecutorWindowAgg::type_decimal128, input_decimal_vec, "avg", 0, 0);
    // TestCase<CountMocker> decimal256_case(ExecutorWindowAgg::type_decimal256, input_decimal_vec, "avg", 0, 0);

    // executeWindowAggTest(int_case);
    // executeWindowAggTest(decimal128_case);
    // executeWindowAggTest(decimal256_case);
}
CATCH

// TODO use unique_ptr in data
TEST_F(ExecutorWindowAgg, Min)
try
{
    // TODO add string type etc... in AggregateFunctionMinMaxAny.h
    TestCase<MinOrMaxMocker<false>>
        int_case(ExecutorWindowAgg::type_int, input_int_vec, "min", std::numeric_limits<Int64>::max(), 0);
    TestCase<MinOrMaxMocker<false>> decimal128_case(
        ExecutorWindowAgg::type_decimal128,
        input_decimal_vec,
        "min",
        std::numeric_limits<Int64>::max(),
        scale);
    TestCase<MinOrMaxMocker<false>> decimal256_case(
        ExecutorWindowAgg::type_decimal256,
        input_decimal_vec,
        "min",
        std::numeric_limits<Int64>::max(),
        scale);

    executeWindowAggTest(int_case);
    executeWindowAggTest(decimal128_case);
    executeWindowAggTest(decimal256_case);
}
CATCH

TEST_F(ExecutorWindowAgg, Max)
try
{
    // TODO add string type etc... in AggregateFunctionMinMaxAny.h
    TestCase<MinOrMaxMocker<true>>
        int_case(ExecutorWindowAgg::type_int, input_int_vec, "max", std::numeric_limits<Int64>::min(), 0);
    TestCase<MinOrMaxMocker<true>> decimal128_case(
        ExecutorWindowAgg::type_decimal128,
        input_decimal_vec,
        "max",
        std::numeric_limits<Int64>::min(),
        scale);
    TestCase<MinOrMaxMocker<true>> decimal256_case(
        ExecutorWindowAgg::type_decimal256,
        input_decimal_vec,
        "max",
        std::numeric_limits<Int64>::min(),
        scale);

    executeWindowAggTest(int_case);
    executeWindowAggTest(decimal128_case);
    executeWindowAggTest(decimal256_case);
}
CATCH

} // namespace tests
} // namespace DB
