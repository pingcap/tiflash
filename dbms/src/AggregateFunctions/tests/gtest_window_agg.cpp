// Copyright 2025 PingCAP, Inc.
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
#include <DataTypes/DataTypeMyDuration.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <TestUtils/AggregationTestUtils.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace tests
{
static const String NULL_VALUE = "NULL";
static const String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()_+";
static const UInt32 CHARACTERS_LEN = CHARACTERS.size();
static constexpr int SCALE = 2;
static std::vector<String> input_decimal_in_string_vec{
    "0",
    "71.94",
    "12.34",
    "-34.26",
    "80.02",
    "-84.39",
    "28.41",
    "45.32",
    "11.11",
    "-10.32",
    "38.21",
    "-11.11",
    "42.01",
    "10.02",
    "43.62",
    "33.33",
    "-90.72"};
static std::vector<String> input_decimal_in_string_vec_aux{
    "0",
    "7194",
    "1234",
    "-3426",
    "8002",
    "-8439",
    "2841",
    "4532",
    "1111",
    "-1032",
    "3821",
    "-1111",
    "4201",
    "1002",
    "4362",
    "3333",
    "-9072"};
static std::vector<Int64> input_int_vec{1, -2, 7, 4, 0, -3, -1, 0, 0, 9, 2, 0, -4, 2, 6, -3, 5};
static std::vector<Int64> input_decimal_vec{
    std::stoi(input_decimal_in_string_vec_aux[0]),
    std::stoi(input_decimal_in_string_vec_aux[1]),
    std::stoi(input_decimal_in_string_vec_aux[2]),
    std::stoi(input_decimal_in_string_vec_aux[3]),
    std::stoi(input_decimal_in_string_vec_aux[4]),
    std::stoi(input_decimal_in_string_vec_aux[5]),
    std::stoi(input_decimal_in_string_vec_aux[6]),
    std::stoi(input_decimal_in_string_vec_aux[7]),
    std::stoi(input_decimal_in_string_vec_aux[8]),
    std::stoi(input_decimal_in_string_vec_aux[9]),
    std::stoi(input_decimal_in_string_vec_aux[10]),
    std::stoi(input_decimal_in_string_vec_aux[11]),
    std::stoi(input_decimal_in_string_vec_aux[12]),
    std::stoi(input_decimal_in_string_vec_aux[13]),
    std::stoi(input_decimal_in_string_vec_aux[14]),
    std::stoi(input_decimal_in_string_vec_aux[15]),
    std::stoi(input_decimal_in_string_vec_aux[16]),
};
static std::vector<String> input_string_vec;
static std::vector<Int64> input_duration_vec{12, 43, 2, 0, 54, 23, 65, 76, 23, 12, 43, 56, 2, 2, 23, 54, 67};

static std::vector<Int32> null_map{0, 1, 0, 1, 0, 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 1, 1};

String eliminateTailing(String str)
{
    int point_idx = -1;
    size_t size = str.size();
    for (size_t i = 0; i < size; i++)
    {
        if (str[i] == '.')
        {
            point_idx = i;
            break;
        }
    }

    // Can't find point
    if (point_idx == -1)
        return str;

    if (static_cast<int>(size) > point_idx + 3)
        return String(str.c_str(), point_idx + 3);
    return str;
}

class MockerBase
{
public:
    explicit MockerBase(Int32 scale_)
        : scale(scale_)
        , counter(0)
    {}

    inline const std::vector<String> & getResults() noexcept { return results; }
    inline static void addString(const String &) { throw Exception("Not implemented yet"); }
    inline static void decreaseString() { throw Exception("Not implemented yet"); }

    inline void addNull() noexcept {}
    inline void decreaseNull() noexcept {}

    inline void resetCounter() noexcept { counter = 0; }
    inline void addCounter() noexcept { counter++; }
    inline void decreaseCounter() noexcept
    {
        counter--;
        assert(counter >= 0);
    }
    inline bool isResultNull() const noexcept { return counter == 0; }

protected:
    inline String convertResIntToString(Int64 res) const noexcept { return Decimal256(res).toString(scale); }

    std::vector<String> results;
    Int32 scale; // scale is 0 when test type is int
    Int32 counter;
};

class SumMocker : public MockerBase
{
public:
    explicit SumMocker(Int32 scale)
        : MockerBase(scale)
    {}

    inline void add(Int64 data) noexcept
    {
        res += data;
        addCounter();
    }

    inline void decrease(Int64 data) noexcept
    {
        res -= data;
        decreaseCounter();
    }

    inline void reset() noexcept
    {
        res = 0;
        resetCounter();
    }

    inline void saveResult() noexcept
    {
        if (isResultNull())
            results.push_back(NULL_VALUE);
        else
            results.push_back(convertResIntToString(res));
    }

private:
    Int64 res{};
};

class CountMocker : public MockerBase
{
public:
    explicit CountMocker(Int32 scale)
        : MockerBase(scale)
    {}

    inline void add(Int64) noexcept { res++; }
    inline void decrease(Int64) noexcept { res--; }
    inline void reset() noexcept { res = 0; }
    inline void saveResult() noexcept { results.push_back(convertResIntToString(res)); }

private:
    Int64 res{};
};

class AvgMocker : public MockerBase
{
public:
    explicit AvgMocker(Int32 scale)
        : MockerBase(scale)
        , sum(0)
        , count(0)
    {}

    inline void add(Int64 data) noexcept
    {
        sum += data;
        count++;
        addCounter();
    }

    inline void decrease(Int64 data) noexcept
    {
        sum -= data;
        count--;
        decreaseCounter();
    }

    inline void reset() noexcept
    {
        sum = 0;
        count = 0;
        resetCounter();
    }

    void saveResult() noexcept
    {
        if (isResultNull())
            results.push_back(NULL_VALUE);
        else
        {
            if (scale == 0)
                results.push_back(std::to_string(avgIntImpl()));
            else
                results.push_back(convertResIntToString(avgDecimalImpl()));
        }
    }

private:
    inline Float64 avgIntImpl() const noexcept { return static_cast<Float64>(sum) / static_cast<Float64>(count); }
    inline Int64 avgDecimalImpl() const noexcept { return sum / count; }

    Int64 sum;
    Int64 count;
};

template <bool is_max>
class MinOrMaxMocker : public MockerBase
{
public:
    explicit MinOrMaxMocker(Int32 scale)
        : MockerBase(scale)
    {}

    inline void add(Int64 data) noexcept
    {
        saved_values.push_back(data);
        addCounter();
    }

    inline void decrease(Int64) noexcept
    {
        saved_values.pop_front();
        decreaseCounter();
    }

    inline void reset() noexcept
    {
        saved_values.clear();
        saved_string_values.clear();
        resetCounter();
    }

    inline void addString(const String & data) noexcept
    {
        saved_string_values.push_back(data);
        addCounter();
    }

    inline void decreaseString() noexcept
    {
        saved_string_values.pop_front();
        decreaseCounter();
    }

    inline void saveResult() noexcept
    {
        if (isResultNull())
            results.push_back(NULL_VALUE);
        else
        {
            // Inefficient, but it's ok in the ut
            if (saved_string_values.empty())
            {
                Int64 res = saved_values[0];
                auto size = saved_values.size();
                for (size_t i = 1; i < size; i++)
                    cmpAndChange(res, saved_values[i]);
                results.push_back(convertResIntToString(res));
            }
            else
            {
                String res = saved_string_values[0];
                auto size = saved_string_values.size();
                for (size_t i = 1; i < size; i++)
                    cmpAndChange(res, saved_string_values[i]);
                results.push_back(res);
            }
        }
    }

private:
    template <typename T>
    static void inline cmpAndChange(T & res, T value) noexcept
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
    std::deque<String> saved_string_values;
};

template <typename OpMocker>
struct TestCase
{
    TestCase(
        DataTypePtr type_,
        const std::vector<Int64> & input_int_vec_,
        const std::vector<String> & input_string_vec_,
        const String & agg_name_,
        int scale_,
        bool has_decrease_ = true)
        : type(type_)
        , input_int_vec(input_int_vec_)
        , input_string_vec(input_string_vec_)
        , agg_name(agg_name_)
        , mocker(scale_)
        , has_decrease(has_decrease_)
    {}

    inline void addInMock(Int64 row_idx) noexcept
    {
        if (input_string_vec.empty())
            mocker.add(input_int_vec[row_idx]);
        else
            mocker.addString(input_string_vec[row_idx]);
    }

    inline void decreaseInMock(Int64 row_idx) noexcept
    {
        if (input_string_vec.empty())
            mocker.decrease(input_int_vec[row_idx]);
        else
            mocker.decreaseString();
    }

    inline void addNullInMock() noexcept { mocker.addNull(); }
    inline void decreaseNullInMock() noexcept { mocker.decreaseNull(); }

    inline UInt32 getRowNum() noexcept
    {
        if (input_string_vec.empty())
            return input_int_vec.size();
        else
            return input_string_vec.size();
    }

    inline void reset() noexcept { mocker.reset(); }
    inline void saveResult() noexcept { mocker.saveResult(); }
    inline const std::vector<String> & getResults() noexcept { return mocker.getResults(); }

    const DataTypePtr type;
    const std::vector<Int64> input_int_vec;
    const std::vector<String> input_string_vec;
    const String agg_name;
    OpMocker mocker;
    bool has_decrease;
};

class ExecutorWindowAgg : public DB::tests::AggregationTest
{
public:
    void SetUp() override { dre = std::default_random_engine(r()); }

    static void SetUpTestCase()
    {
        // Ensure all vectors have same size so that we can use the same null map
        assert(input_int_vec.size() == input_decimal_in_string_vec_aux.size());
        assert(input_int_vec.size() == input_decimal_in_string_vec.size());
        assert(input_int_vec.size() == input_decimal_vec.size());
        assert(input_int_vec.size() == input_duration_vec.size());
        assert(input_int_vec.size() == null_map.size());

        AggregationTest::SetUpTestCase();

        std::random_device r;
        std::default_random_engine dre(r());
        std::uniform_int_distribution<UInt32> di;

        di.param(std::uniform_int_distribution<UInt32>::param_type{5, 15});
        for (UInt32 i = 0; i < input_int_vec.size(); i++)
        {
            di.param(std::uniform_int_distribution<UInt32>::param_type{0, 64});
            auto len = di(dre);
            di.param(std::uniform_int_distribution<UInt32>::param_type{0, CHARACTERS_LEN - 1});

            String str;
            for (UInt32 j = 0; j < len; j++)
            {
                auto idx = di(dre);
                str += CHARACTERS[idx];
            }
            input_string_vec.push_back(str);
        }

        assert(input_int_vec.size() == input_string_vec.size());

        input_int_col = createColumn<Int64>(input_int_vec).column;
        input_decimal128_col = createColumn<Decimal128>(std::make_tuple(10, SCALE), input_decimal_in_string_vec).column;
        input_decimal256_col = createColumn<Decimal256>(std::make_tuple(30, SCALE), input_decimal_in_string_vec).column;
        input_string_col = createColumn<String>(input_string_vec).column;
        input_duration_col = createColumn<Int64>(input_duration_vec).column;
        input_nullable_int_col = createNullableColumn<Int64>(input_int_vec, null_map).column;
        input_nullable_decimal128_col
            = createNullableColumn<Decimal128>(std::make_tuple(10, SCALE), input_decimal_in_string_vec, null_map)
                  .column;
        input_nullable_decimal256_col
            = createNullableColumn<Decimal256>(std::make_tuple(30, SCALE), input_decimal_in_string_vec, null_map)
                  .column;
        input_nullable_string_col = createNullableColumn<String>(input_string_vec, null_map).column;
        input_nullable_duration_col = createNullableColumn<Int64>(input_duration_vec, null_map).column;

        type_nullable_int = std::make_shared<DataTypeNullable>(type_int);
        type_nullable_decimal128 = std::make_shared<DataTypeNullable>(type_decimal128);
        type_nullable_decimal256 = std::make_shared<DataTypeNullable>(type_decimal256);
        type_nullable_string = std::make_shared<DataTypeNullable>(type_string);
        type_nullable_duration = std::make_shared<DataTypeNullable>(type_duration);
    }

private:
    // range: [begin, end]
    inline UInt32 rand(UInt32 begin, UInt32 end) noexcept
    {
        di.param(std::uniform_int_distribution<UInt32>::param_type{begin, end});
        return di(dre);
    }

protected:
    template <typename Op, bool has_null>
    void executeWindowAggTest(TestCase<Op> & test_case);

    static const IColumn * getInputColumn(const IDataType * type)
    {
        if (const auto * tmp = dynamic_cast<const DataTypeInt64 *>(type); tmp != nullptr)
            return &(*ExecutorWindowAgg::input_int_col);
        else if (const auto * tmp = dynamic_cast<const DataTypeDecimal128 *>(type); tmp != nullptr)
            return &(*ExecutorWindowAgg::input_decimal128_col);
        else if (const auto * tmp = dynamic_cast<const DataTypeDecimal256 *>(type); tmp != nullptr)
            return &(*ExecutorWindowAgg::input_decimal256_col);
        else if (const auto * tmp = dynamic_cast<const DataTypeString *>(type); tmp != nullptr)
            return &(*ExecutorWindowAgg::input_string_col);
        else if (const auto * tmp = dynamic_cast<const DataTypeMyDuration *>(type); tmp != nullptr)
            return &(*ExecutorWindowAgg::input_duration_col);
        else if (const auto * tmp = dynamic_cast<const DataTypeNullable *>(type); tmp != nullptr)
        {
            const auto * nested_type = tmp->getNestedType().get();
            if (const auto * nested_tmp = dynamic_cast<const DataTypeInt64 *>(nested_type); nested_tmp != nullptr)
                return &(*ExecutorWindowAgg::input_nullable_int_col);
            else if (const auto * nested_tmp = dynamic_cast<const DataTypeDecimal128 *>(nested_type);
                     nested_tmp != nullptr)
                return &(*ExecutorWindowAgg::input_nullable_decimal128_col);
            else if (const auto * nested_tmp = dynamic_cast<const DataTypeDecimal256 *>(nested_type);
                     nested_tmp != nullptr)
                return &(*ExecutorWindowAgg::input_nullable_decimal256_col);
            else if (const auto * nested_tmp = dynamic_cast<const DataTypeString *>(nested_type); nested_tmp != nullptr)
                return &(*ExecutorWindowAgg::input_nullable_string_col);
            else if (const auto * nested_tmp = dynamic_cast<const DataTypeMyDuration *>(nested_type);
                     nested_tmp != nullptr)
                return &(*ExecutorWindowAgg::input_nullable_duration_col);
            else
                throw Exception(fmt::format("Invalid nested data type {}", nested_type->getName()));
        }
        else
            throw Exception(fmt::format("Invalid data type {}", type->getName()));
    }

    static String getValue(const Field & field)
    {
        switch (field.getType())
        {
        case Field::Types::Which::Int64:
            return std::to_string(field.template get<Int64>());
        case Field::Types::Which::UInt64:
            return std::to_string(field.template get<UInt64>());
        case Field::Types::Which::Float64:
            return std::to_string(field.template get<Float64>());
        case Field::Types::Which::String:
            return field.template get<String>();
        case Field::Types::Which::Decimal64:
            return eliminateTailing(field.template get<DecimalField<Decimal64>>().toString());
        case Field::Types::Which::Decimal128:
            return eliminateTailing(field.template get<DecimalField<Decimal128>>().toString());
        case Field::Types::Which::Decimal256:
            return eliminateTailing(field.template get<DecimalField<Decimal256>>().toString());
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

    static ColumnPtr input_int_col;
    static ColumnPtr input_decimal128_col;
    static ColumnPtr input_decimal256_col;
    static ColumnPtr input_string_col;
    static ColumnPtr input_duration_col;

    static ColumnPtr input_nullable_int_col;
    static ColumnPtr input_nullable_decimal128_col;
    static ColumnPtr input_nullable_decimal256_col;
    static ColumnPtr input_nullable_string_col;
    static ColumnPtr input_nullable_duration_col;

    static DataTypePtr type_int;
    static DataTypePtr type_decimal128;
    static DataTypePtr type_decimal256;
    static DataTypePtr type_string;
    static DataTypePtr type_duration;

    static DataTypePtr type_nullable_int;
    static DataTypePtr type_nullable_decimal128;
    static DataTypePtr type_nullable_decimal256;
    static DataTypePtr type_nullable_string;
    static DataTypePtr type_nullable_duration;
};

ColumnPtr ExecutorWindowAgg::input_int_col;
ColumnPtr ExecutorWindowAgg::input_decimal128_col;
ColumnPtr ExecutorWindowAgg::input_decimal256_col;
ColumnPtr ExecutorWindowAgg::input_string_col;
ColumnPtr ExecutorWindowAgg::input_duration_col;

ColumnPtr ExecutorWindowAgg::input_nullable_int_col;
ColumnPtr ExecutorWindowAgg::input_nullable_decimal128_col;
ColumnPtr ExecutorWindowAgg::input_nullable_decimal256_col;
ColumnPtr ExecutorWindowAgg::input_nullable_string_col;
ColumnPtr ExecutorWindowAgg::input_nullable_duration_col;

DataTypePtr ExecutorWindowAgg::type_int = std::make_shared<DataTypeInt64>();
DataTypePtr ExecutorWindowAgg::type_decimal128 = std::make_shared<DataTypeDecimal128>(10, SCALE);
DataTypePtr ExecutorWindowAgg::type_decimal256 = std::make_shared<DataTypeDecimal256>(30, SCALE);
DataTypePtr ExecutorWindowAgg::type_string = std::make_shared<DataTypeString>();
DataTypePtr ExecutorWindowAgg::type_duration = std::make_shared<DataTypeMyDuration>();

DataTypePtr ExecutorWindowAgg::type_nullable_int;
DataTypePtr ExecutorWindowAgg::type_nullable_decimal128;
DataTypePtr ExecutorWindowAgg::type_nullable_decimal256;
DataTypePtr ExecutorWindowAgg::type_nullable_string;
DataTypePtr ExecutorWindowAgg::type_nullable_duration;

template <typename Op, bool has_null>
void ExecutorWindowAgg::executeWindowAggTest(TestCase<Op> & test_case)
{
    Arena arena;
    auto context = TiFlashTestEnv::getContext();
    std::deque<int> added_row_idx_queue;

    added_row_idx_queue.clear();
    auto agg_func
        = AggregateFunctionFactory::instance().getForWindow(*context, test_case.agg_name, {test_case.type}, {}, true);
    auto return_type = agg_func->getReturnType();
    AlignedBuffer agg_state;
    agg_state.reset(agg_func->sizeOfData(), agg_func->alignOfData());
    agg_func->create(agg_state.data());

    const UInt32 col_row_num = test_case.getRowNum();

    UInt32 reset_num = getResetNum();
    auto res_col = return_type->createColumn();
    const IColumn * input_col = getInputColumn(test_case.type.get());

    // Start test
    for (UInt32 i = 0; i < reset_num; i++)
    {
        test_case.reset();
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
                if (has_null && null_map[row_idx] == 1)
                    test_case.addNullInMock();
                else
                    test_case.addInMock(row_idx);
            }

            if likely (!added_row_idx_queue.empty())
            {
                // Start to decrease
                UInt32 decrease_num = 0;
                if (test_case.has_decrease)
                    decrease_num = getDecreaseNum(added_row_idx_queue.size() - 1);

                for (UInt32 k = 0; k < decrease_num; k++)
                {
                    const UInt32 row_idx = added_row_idx_queue.front();
                    added_row_idx_queue.pop_front();
                    agg_func->decrease(agg_state.data(), &input_col, row_idx, &arena);
                    if (has_null && null_map[row_idx] == 1)
                        test_case.decreaseNullInMock();
                    else
                        test_case.decreaseInMock(row_idx);
                }
            }

            agg_func->insertResultInto(agg_state.data(), *res_col, &arena);
            test_case.saveResult();
        }
    }

    const std::vector<String> res_vec = test_case.getResults();
    size_t res_num = res_vec.size();
    ASSERT_EQ(res_num, res_col->size());

    Field res_field;
    for (size_t i = 0; i < res_num; i++)
    {
        res_col->get(i, res_field);
        if (res_vec[i] == NULL_VALUE)
        {
            ASSERT_TRUE(res_field.isNull());
            continue;
        }
        ASSERT_FALSE(res_field.isNull());
        // No matter what type the result is, we always use decimal to convert the result to string so that it's easy to check result
        ASSERT_EQ(res_vec[i], getValue(res_field));
    }
}

TEST_F(ExecutorWindowAgg, Sum)
try
{
    TestCase<SumMocker> int_case(ExecutorWindowAgg::type_int, input_int_vec, {}, "sum", 0);
    TestCase<SumMocker> decimal128_case(ExecutorWindowAgg::type_decimal128, input_decimal_vec, {}, "sum", SCALE);
    TestCase<SumMocker> decimal256_case(ExecutorWindowAgg::type_decimal256, input_decimal_vec, {}, "sum", SCALE);

    executeWindowAggTest<SumMocker, false>(int_case);
    executeWindowAggTest<SumMocker, false>(decimal128_case);
    executeWindowAggTest<SumMocker, false>(decimal256_case);

    TestCase<SumMocker> int_nullable_case(ExecutorWindowAgg::type_nullable_int, input_int_vec, {}, "sum", 0);
    TestCase<SumMocker>
        decimal128_nullable_case(ExecutorWindowAgg::type_nullable_decimal128, input_decimal_vec, {}, "sum", SCALE);
    TestCase<SumMocker>
        decimal256_nullable_case(ExecutorWindowAgg::type_nullable_decimal256, input_decimal_vec, {}, "sum", SCALE);

    executeWindowAggTest<SumMocker, true>(int_nullable_case);
    executeWindowAggTest<SumMocker, true>(decimal128_nullable_case);
    executeWindowAggTest<SumMocker, true>(decimal256_nullable_case);
}
CATCH

TEST_F(ExecutorWindowAgg, Count)
try
{
    TestCase<CountMocker> int_case(ExecutorWindowAgg::type_int, input_int_vec, {}, "count", 0);
    TestCase<CountMocker> decimal128_case(ExecutorWindowAgg::type_decimal128, input_decimal_vec, {}, "count", 0);
    TestCase<CountMocker> decimal256_case(ExecutorWindowAgg::type_decimal256, input_decimal_vec, {}, "count", 0);

    executeWindowAggTest<CountMocker, false>(int_case);
    executeWindowAggTest<CountMocker, false>(decimal128_case);
    executeWindowAggTest<CountMocker, false>(decimal256_case);

    TestCase<CountMocker> int_nullable_case(ExecutorWindowAgg::type_nullable_int, input_int_vec, {}, "count", 0);
    TestCase<CountMocker>
        decimal128_nullable_case(ExecutorWindowAgg::type_nullable_decimal128, input_decimal_vec, {}, "count", 0);
    TestCase<CountMocker>
        decimal256_nullable_case(ExecutorWindowAgg::type_nullable_decimal256, input_decimal_vec, {}, "count", 0);

    executeWindowAggTest<CountMocker, true>(int_nullable_case);
    executeWindowAggTest<CountMocker, true>(decimal128_nullable_case);
    executeWindowAggTest<CountMocker, true>(decimal256_nullable_case);
}
CATCH

TEST_F(ExecutorWindowAgg, Avg)
try
{
    TestCase<AvgMocker> int_case(ExecutorWindowAgg::type_int, input_int_vec, {}, "avg", 0);
    TestCase<AvgMocker> decimal128_case(ExecutorWindowAgg::type_decimal128, input_decimal_vec, {}, "avg", SCALE);
    TestCase<AvgMocker> decimal256_case(ExecutorWindowAgg::type_decimal256, input_decimal_vec, {}, "avg", SCALE);

    executeWindowAggTest<AvgMocker, false>(int_case);
    executeWindowAggTest<AvgMocker, false>(decimal128_case);
    executeWindowAggTest<AvgMocker, false>(decimal256_case);

    TestCase<AvgMocker> int_nullable_case(ExecutorWindowAgg::type_nullable_int, input_int_vec, {}, "avg", 0);
    TestCase<AvgMocker>
        decimal128_nullable_case(ExecutorWindowAgg::type_nullable_decimal128, input_decimal_vec, {}, "avg", SCALE);
    TestCase<AvgMocker>
        decimal256_nullable_case(ExecutorWindowAgg::type_nullable_decimal256, input_decimal_vec, {}, "avg", SCALE);

    executeWindowAggTest<AvgMocker, true>(int_nullable_case);
    executeWindowAggTest<AvgMocker, true>(decimal128_nullable_case);
    executeWindowAggTest<AvgMocker, true>(decimal256_nullable_case);
}
CATCH

TEST_F(ExecutorWindowAgg, Min)
try
{
    const std::vector<int> decreases{true, false};
    for (auto has_decrease : decreases)
    {
        String agg_func_name;
        if (has_decrease)
            agg_func_name = "min_for_window";
        else
            agg_func_name = "min";
        TestCase<MinOrMaxMocker<false>>
            int_case(ExecutorWindowAgg::type_int, input_int_vec, {}, agg_func_name, 0, has_decrease);
        TestCase<MinOrMaxMocker<false>> decimal128_case(
            ExecutorWindowAgg::type_decimal128,
            input_decimal_vec,
            {},
            agg_func_name,
            SCALE,
            has_decrease);
        TestCase<MinOrMaxMocker<false>> decimal256_case(
            ExecutorWindowAgg::type_decimal256,
            input_decimal_vec,
            {},
            agg_func_name,
            SCALE,
            has_decrease);
        TestCase<MinOrMaxMocker<false>>
            string_case(ExecutorWindowAgg::type_string, {}, input_string_vec, agg_func_name, 0, has_decrease);
        TestCase<MinOrMaxMocker<false>>
            duration_case(ExecutorWindowAgg::type_duration, input_duration_vec, {}, agg_func_name, 0, has_decrease);

        executeWindowAggTest<MinOrMaxMocker<false>, false>(int_case);
        executeWindowAggTest<MinOrMaxMocker<false>, false>(decimal128_case);
        executeWindowAggTest<MinOrMaxMocker<false>, false>(decimal256_case);
        executeWindowAggTest<MinOrMaxMocker<false>, false>(string_case);
        executeWindowAggTest<MinOrMaxMocker<false>, false>(duration_case);

        TestCase<MinOrMaxMocker<false>>
            int_nullable_case(ExecutorWindowAgg::type_nullable_int, input_int_vec, {}, agg_func_name, 0, has_decrease);
        TestCase<MinOrMaxMocker<false>> decimal128_nullable_case(
            ExecutorWindowAgg::type_nullable_decimal128,
            input_decimal_vec,
            {},
            agg_func_name,
            SCALE,
            has_decrease);
        TestCase<MinOrMaxMocker<false>> decimal256_nullable_case(
            ExecutorWindowAgg::type_nullable_decimal256,
            input_decimal_vec,
            {},
            agg_func_name,
            SCALE,
            has_decrease);
        TestCase<MinOrMaxMocker<false>> string_nullable_case(
            ExecutorWindowAgg::type_nullable_string,
            {},
            input_string_vec,
            agg_func_name,
            0,
            has_decrease);
        TestCase<MinOrMaxMocker<false>> duration_nullable_case(
            ExecutorWindowAgg::type_nullable_duration,
            input_duration_vec,
            {},
            agg_func_name,
            0,
            has_decrease);

        executeWindowAggTest<MinOrMaxMocker<false>, true>(int_nullable_case);
        executeWindowAggTest<MinOrMaxMocker<false>, true>(decimal128_nullable_case);
        executeWindowAggTest<MinOrMaxMocker<false>, true>(decimal256_nullable_case);
        executeWindowAggTest<MinOrMaxMocker<false>, true>(string_nullable_case);
        executeWindowAggTest<MinOrMaxMocker<false>, true>(duration_nullable_case);
    }
}
CATCH

TEST_F(ExecutorWindowAgg, Max)
try
{
    const std::vector<int> decreases{true, false};
    for (auto has_decrease : decreases)
    {
        String agg_func_name;
        if (has_decrease)
            agg_func_name = "max_for_window";
        else
            agg_func_name = "max";
        TestCase<MinOrMaxMocker<true>>
            int_case(ExecutorWindowAgg::type_int, input_int_vec, {}, agg_func_name, 0, has_decrease);
        TestCase<MinOrMaxMocker<true>> decimal128_case(
            ExecutorWindowAgg::type_decimal128,
            input_decimal_vec,
            {},
            agg_func_name,
            SCALE,
            has_decrease);
        TestCase<MinOrMaxMocker<true>> decimal256_case(
            ExecutorWindowAgg::type_decimal256,
            input_decimal_vec,
            {},
            agg_func_name,
            SCALE,
            has_decrease);
        TestCase<MinOrMaxMocker<true>>
            string_case(ExecutorWindowAgg::type_string, {}, input_string_vec, agg_func_name, 0, has_decrease);
        TestCase<MinOrMaxMocker<true>>
            duration_case(ExecutorWindowAgg::type_duration, input_duration_vec, {}, agg_func_name, 0, has_decrease);

        executeWindowAggTest<MinOrMaxMocker<true>, false>(int_case);
        executeWindowAggTest<MinOrMaxMocker<true>, false>(decimal128_case);
        executeWindowAggTest<MinOrMaxMocker<true>, false>(decimal256_case);
        executeWindowAggTest<MinOrMaxMocker<true>, false>(string_case);
        executeWindowAggTest<MinOrMaxMocker<true>, false>(duration_case);

        TestCase<MinOrMaxMocker<true>>
            int_nullable_case(ExecutorWindowAgg::type_nullable_int, input_int_vec, {}, agg_func_name, 0, has_decrease);
        TestCase<MinOrMaxMocker<true>> decimal128_nullable_case(
            ExecutorWindowAgg::type_nullable_decimal128,
            input_decimal_vec,
            {},
            agg_func_name,
            SCALE,
            has_decrease);
        TestCase<MinOrMaxMocker<true>> decimal256_nullable_case(
            ExecutorWindowAgg::type_nullable_decimal256,
            input_decimal_vec,
            {},
            agg_func_name,
            SCALE,
            has_decrease);
        TestCase<MinOrMaxMocker<true>> string_nullable_case(
            ExecutorWindowAgg::type_nullable_string,
            {},
            input_string_vec,
            agg_func_name,
            0,
            has_decrease);
        TestCase<MinOrMaxMocker<true>> duration_nullable_case(
            ExecutorWindowAgg::type_nullable_duration,
            input_duration_vec,
            {},
            agg_func_name,
            0,
            has_decrease);

        executeWindowAggTest<MinOrMaxMocker<true>, true>(int_nullable_case);
        executeWindowAggTest<MinOrMaxMocker<true>, true>(decimal128_nullable_case);
        executeWindowAggTest<MinOrMaxMocker<true>, true>(decimal256_nullable_case);
        executeWindowAggTest<MinOrMaxMocker<true>, true>(string_nullable_case);
        executeWindowAggTest<MinOrMaxMocker<true>, true>(duration_nullable_case);
    }
}
CATCH

} // namespace tests
} // namespace DB
