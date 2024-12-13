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
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <TestUtils/AggregationTestUtils.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{
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
    inline UInt32 getResetNum() noexcept { return rand(1, 10); }
    inline UInt32 getAddNum() noexcept { return rand(1, 5); }
    inline UInt32 getDecreaseNum(UInt32 max_num) noexcept { return rand(0, max_num); }
    inline UInt32 getRowIdx(UInt32 start, UInt32 end) noexcept { return rand(start, end); }
    inline UInt32 getResultNum() noexcept { return rand(1, 3); }

    std::random_device r;
    std::default_random_engine dre;
    std::uniform_int_distribution<UInt32> di;

    static const ColumnPtr input_int_col;
    static const ColumnPtr input_decimal64_col;
    static const ColumnPtr input_decimal256_col;

    static DataTypePtr type_int;
    static DataTypePtr type_decimal64;
    static DataTypePtr type_decimal256;
};

const std::vector<Int64> input_int_vec{1, -2, 7, 4, 0, -3, -1, 0, 0, 9, 2, 0, -4, 2, 6, -3, 5};
const std::vector<String> input_decimal_vec{"0.12", "0", "1.11", "0", "-0.23", "0", "0", "-0.98", "1.21"};

const ColumnPtr ExecutorWindowAgg::input_int_col = createColumn<Int64>(input_int_vec).column;
const ColumnPtr ExecutorWindowAgg::input_decimal64_col
    = createColumn<Decimal64>(std::make_tuple(9, 4), input_decimal_vec).column;
const ColumnPtr ExecutorWindowAgg::input_decimal256_col
    = createColumn<Decimal256>(std::make_tuple(9, 4), input_decimal_vec).column;

DataTypePtr ExecutorWindowAgg::type_int = std::make_shared<DataTypeInt64>();
DataTypePtr ExecutorWindowAgg::type_decimal64 = std::make_shared<DataTypeDecimal64>();
DataTypePtr ExecutorWindowAgg::type_decimal256 = std::make_shared<DataTypeDecimal256>();

TEST_F(ExecutorWindowAgg, Sum)
try
{
    Arena arena;
    auto context = TiFlashTestEnv::getContext();
    auto agg_func
        = AggregateFunctionFactory::instance().get(*context, "sum", {ExecutorWindowAgg::type_int}, {}, 0, true);
    AlignedBuffer agg_state;
    agg_state.reset(agg_func->sizeOfData(), agg_func->alignOfData());
    agg_func->create(agg_state.data());

    std::deque<int> added_row_idx_queue;
    std::vector<Int64> res_int;
    res_int.reserve(10);

    const UInt32 col_int_size = input_int_vec.size();

    UInt32 reset_num = getResetNum();
    auto res_col = ColumnInt64::create();
    auto null_map_col = ColumnUInt8::create();
    auto null_res_col = ColumnNullable::create(std::move(res_col), std::move(null_map_col));
    const IColumn * int_col = &(*ExecutorWindowAgg::input_int_col);

    {
        // Test for int
        for (UInt32 i = 0; i < reset_num; i++)
        {
            Int64 res = 0;
            agg_func->reset(agg_state.data());

            const UInt32 res_num = getResultNum();
            for (UInt32 j = 0; j < res_num; j++)
            {
                const UInt32 add_num = getAddNum();
                for (UInt32 k = 0; k < add_num; k++)
                {
                    const UInt32 row_idx = getRowIdx(0, col_int_size - 1);
                    added_row_idx_queue.push_back(row_idx);
                    agg_func->add(agg_state.data(), &int_col, row_idx, &arena);
                    res += input_int_vec[row_idx];
                }

                const UInt32 decrease_num = getDecreaseNum(add_num); // todo change to length of deque
                for (UInt32 k = 0; k < decrease_num; k++)
                {
                    const UInt32 row_idx = added_row_idx_queue.front();
                    added_row_idx_queue.pop_front();
                    agg_func->decrease(agg_state.data(), &int_col, row_idx, &arena);
                    res -= input_int_vec[row_idx];
                }

                agg_func->insertResultInto(agg_state.data(), *null_res_col, &arena);
                res_int.push_back(res);
            }
        }

        const auto nested_res_col = null_res_col->getNestedColumnPtr();
        size_t res_num = res_int.size();
        ASSERT_EQ(res_num, null_res_col->size());
        for (size_t i = 0; i < res_num; i++)
        {
            ASSERT_FALSE(null_res_col->isNullAt(i));
            ASSERT_EQ(res_int[i], nested_res_col->getInt(i));
        }
    }
}
CATCH

TEST_F(ExecutorWindowAgg, Count)
try
{}
CATCH

TEST_F(ExecutorWindowAgg, Avg)
try
{}
CATCH

TEST_F(ExecutorWindowAgg, Min)
try
{}
CATCH

TEST_F(ExecutorWindowAgg, Max)
try
{}
CATCH

} // namespace tests
} // namespace DB
