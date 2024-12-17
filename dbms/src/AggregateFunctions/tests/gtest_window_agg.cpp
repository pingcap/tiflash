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

namespace DB
{
namespace tests
{
constexpr int scale = 2;
const std::vector<String> input_string_vec{"0", "71.94", "12.34", "-34.26", "80.02", "-84.39", "28.41", "45.32"};
const std::vector<String> input_string_vec_aux{"0", "7194", "1234", "-3426", "8002", "-8439", "2841", "4532"};
const std::vector<Int64> input_int_vec{1, -2, 7, 4, 0, -3, -1, 0, 0, 9, 2, 0, -4, 2, 6, -3, 5};
const std::vector<Decimal256> input_decimal_vec{
    Decimal256(std::stoi(input_string_vec_aux[0])),
    Decimal256(std::stoi(input_string_vec_aux[1])),
    Decimal256(std::stoi(input_string_vec_aux[2])),
    Decimal256(std::stoi(input_string_vec_aux[3])),
    Decimal256(std::stoi(input_string_vec_aux[4])),
    Decimal256(std::stoi(input_string_vec_aux[5])),
    Decimal256(std::stoi(input_string_vec_aux[6])),
    Decimal256(std::stoi(input_string_vec_aux[7]))};

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

TEST_F(ExecutorWindowAgg, Sum)
try
{
    Arena arena;
    auto context = TiFlashTestEnv::getContext();
    std::deque<int> added_row_idx_queue;

    {
        // Test for int
        added_row_idx_queue.clear();
        auto agg_func
            = AggregateFunctionFactory::instance().get(*context, "sum", {ExecutorWindowAgg::type_int}, {}, 0, true);
        AlignedBuffer agg_state;
        agg_state.reset(agg_func->sizeOfData(), agg_func->alignOfData());
        agg_func->create(agg_state.data());

        std::vector<Int64> res_int;
        res_int.reserve(10);

        const UInt32 col_int_size = input_int_vec.size();

        UInt32 reset_num = getResetNum();
        auto res_col = ExecutorWindowAgg::type_int->createColumn();
        auto null_map_col = ColumnUInt8::create();
        auto null_res_col = ColumnNullable::create(std::move(res_col), std::move(null_map_col));
        const IColumn * input_col = getInputColumn(ExecutorWindowAgg::type_int.get());

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
                    agg_func->add(agg_state.data(), &input_col, row_idx, &arena);
                    res += input_int_vec[row_idx];
                }

                const UInt32 decrease_num = getDecreaseNum(added_row_idx_queue.size());
                for (UInt32 k = 0; k < decrease_num; k++)
                {
                    const UInt32 row_idx = added_row_idx_queue.front();
                    added_row_idx_queue.pop_front();
                    agg_func->decrease(agg_state.data(), &input_col, row_idx, &arena);
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

    {
        // Test for decimal
        DataTypes test_types{type_decimal128, type_decimal256};
        for (const auto & type : test_types)
        {
            added_row_idx_queue.clear();
            auto agg_func = AggregateFunctionFactory::instance().get(*context, "sum", {type}, {}, 0, true);
            AlignedBuffer agg_state;
            agg_state.reset(agg_func->sizeOfData(), agg_func->alignOfData());
            agg_func->create(agg_state.data());

            std::vector<String> res_vec;
            res_vec.reserve(10);

            const UInt32 col_decimal_size = input_decimal_vec.size();

            UInt32 reset_num = getResetNum();
            auto res_col = type->createColumn();
            auto null_map_col = ColumnUInt8::create();
            auto null_res_col = ColumnNullable::create(std::move(res_col), std::move(null_map_col));
            const IColumn * input_col = getInputColumn(type.get());

            for (UInt32 i = 0; i < reset_num; i++)
            {
                Decimal256 res(0);
                agg_func->reset(agg_state.data());

                const UInt32 res_num = getResultNum();
                for (UInt32 j = 0; j < res_num; j++)
                {
                    const UInt32 add_num = getAddNum();
                    for (UInt32 k = 0; k < add_num; k++)
                    {
                        const UInt32 row_idx = getRowIdx(0, col_decimal_size - 1);
                        added_row_idx_queue.push_back(row_idx);
                        agg_func->add(agg_state.data(), &input_col, row_idx, &arena);
                        res += input_decimal_vec[row_idx];
                    }

                    const UInt32 decrease_num = getDecreaseNum(added_row_idx_queue.size());
                    for (UInt32 k = 0; k < decrease_num; k++)
                    {
                        const UInt32 row_idx = added_row_idx_queue.front();
                        added_row_idx_queue.pop_front();
                        agg_func->decrease(agg_state.data(), &input_col, row_idx, &arena);
                        res -= input_decimal_vec[row_idx];
                    }

                    agg_func->insertResultInto(agg_state.data(), *null_res_col, &arena);
                    res_vec.push_back(res.toString(scale));
                }
            }

            const auto nested_res_col = null_res_col->getNestedColumnPtr();
            size_t res_num = res_vec.size();
            ASSERT_EQ(res_num, null_res_col->size());

            Field res_field;
            for (size_t i = 0; i < res_num; i++)
            {
                ASSERT_FALSE(null_res_col->isNullAt(i));
                nested_res_col->get(i, res_field);
                ASSERT_EQ(res_vec[i], getValue(res_field));
            }
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
