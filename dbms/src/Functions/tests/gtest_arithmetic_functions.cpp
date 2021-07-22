#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <Functions/FunctionFactory.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Context.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>
#include <unordered_map>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <gtest/gtest.h>

#pragma GCC diagnostic pop

namespace DB
{
namespace tests
{

class TestBinaryArithmeticFunctions : public ::testing::Test
{
protected:
    template <typename T>
    using DataVector = std::vector<std::optional<T>>;

    using DecimalField32 = DecimalField<Decimal32>;
    using DecimalField64 = DecimalField<Decimal64>;
    using DecimalField128 = DecimalField<Decimal128>;
    using DecimalField256 = DecimalField<Decimal256>;

    template <typename T, ScaleType scale>
    struct GetValue
    {
        // only decimals have scale.
        static_assert(scale == 0);

        static constexpr T Max() { return std::numeric_limits<T>::max(); }
        static constexpr T Zero() { return 0; }
        static constexpr T One() { return 1; }
    };

    template <typename TDecimal, ScaleType scale>
    struct GetValue<DecimalField<TDecimal>, scale>
    {
        using ReturnType = DecimalField<TDecimal>;

        static constexpr PrecType MaxPrecision = maxDecimalPrecision<TDecimal>();
        static_assert(MaxPrecision > 0);

        static constexpr ReturnType Max()
        {
            TDecimal value(0);
            for (PrecType i = 0; i < MaxPrecision; ++i)
            {
                value *= TDecimal(10);
                value += TDecimal(9);
            }
            return ReturnType(value, scale);
        }

        static constexpr ReturnType Zero() { return ReturnType(TDecimal(0), scale); }

        static constexpr ReturnType One()
        {
            TDecimal value(1);
            for (ScaleType i = 0; i < scale; ++i)
                value *= TDecimal(10);
            return ReturnType(value, scale);
        }
    };

    static void SetUpTestCase()
    {
        try
        {
            registerFunctions();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registed, ignore exception here.
        }
    }

    void executeFunction(Block & block, ColumnWithTypeAndName & c1, ColumnWithTypeAndName & c2, const String & func_name)
    {
        const auto context = TiFlashTestEnv::getContext();
        auto & factory = FunctionFactory::instance();

        ColumnsWithTypeAndName ctns{c1, c2};

        block.insert(c1);
        block.insert(c2);
        ColumnNumbers cns{0, 1};

        auto bp = factory.tryGet(func_name, context);
        ASSERT_TRUE(bp != nullptr);
        auto func = bp->build(ctns);
        block.insert({nullptr, func->getReturnType(), "res"});
        bp->build(ctns)->execute(block, cns, 2);
    }
    void checkNullConstantResult(Block & block, size_t size)
    {
        const IColumn * res_col = block.getByPosition(2).column.get();
        ASSERT_TRUE(size == res_col->size());
        Field res_field;
        for (size_t i = 0; i < size; i++)
        {
            res_col->get(i, res_field);
            ASSERT_TRUE(res_field.isNull());
        }
    }

    template <typename DataType, typename... Args>
    DataTypePtr makeDataType(const Args &... args)
    {
        if constexpr (IsDecimal<DataType>)
            return std::make_shared<DataTypeDecimal<DataType>>(args...);
        else
            return std::make_shared<DataType>(args...);
    }

    template <typename FieldType>
    Field makeField(const std::optional<FieldType> & value)
    {
        if (value.has_value())
            return Field(static_cast<FieldType>(value.value()));
        else
            return Null();
    }

    template <typename FieldType>
    ColumnWithTypeAndName makeInputColumn(
        const String & name, size_t size, const DataTypePtr data_type, const DataVector<FieldType> & column_data)
    {
        auto nullable_data_type = makeNullable(data_type);

        if (column_data.size() == 1)
        {
            auto column = nullable_data_type->createColumnConst(size, makeField(column_data[0]));
            return ColumnWithTypeAndName(std::move(column), nullable_data_type, name);
        }
        else
        {
            auto column = nullable_data_type->createColumn();

            for (auto & data : column_data)
            {
                column->insert(makeField(data));
            }

            return ColumnWithTypeAndName(std::move(column), nullable_data_type, name);
        }
    }

    // e.g. data_type = DataTypeUInt64, FieldType = UInt64.
    // if data vector contains only 1 element, a const column will be created.
    // otherwise, two columns are expected to be of the same size.
    // use std::nullopt for null values.
    template <typename FieldType1, typename FieldType2, typename ResultFieldType>
    void executeFunctionWithData(size_t line, const String & function_name, const DataTypePtr data_type_1, const DataTypePtr data_type_2,
        const DataVector<FieldType1> & column_data_1, const DataVector<FieldType2> & column_data_2,
        const DataVector<ResultFieldType> & expected_data, PrecType expected_prec = 0, ScaleType expected_scale = 0)
    {
        static_assert(std::is_integral_v<FieldType1> || std::is_floating_point_v<FieldType1> || isDecimalField<FieldType1>());
        static_assert(std::is_integral_v<FieldType2> || std::is_floating_point_v<FieldType2> || isDecimalField<FieldType2>());
        static_assert(std::is_integral_v<ResultFieldType> || std::is_floating_point_v<ResultFieldType> || isDecimalField<ResultFieldType>());

        size_t size = std::max(column_data_1.size(), column_data_2.size());
        ASSERT_GT(size, 0) << "at line " << line;
        ASSERT_TRUE(column_data_1.size() == size || column_data_1.size() == 1) << "at line " << line;
        ASSERT_TRUE(column_data_2.size() == size || column_data_2.size() == 1) << "at line " << line;
        ASSERT_EQ(size, expected_data.size()) << "at line " << line;

        auto input_1 = makeInputColumn("input_1", size, data_type_1, column_data_1);
        auto input_2 = makeInputColumn("input_2", size, data_type_2, column_data_2);

        Block block;
        executeFunction(block, input_1, input_2, function_name);

        auto result_column = block.getByName("res").column.get();
        ASSERT_EQ(size, result_column->size()) << "at line " << line;

        auto result_type = block.getByName("res").type;
        if (auto ptr = typeid_cast<const DataTypeNullable *>(result_type.get()))
            result_type = ptr->getNestedType();

        if (IsDecimalDataType(result_type))
        {
            ASSERT_EQ(expected_prec, getDecimalPrecision(*result_type, 0)) << "at line " << line;
            ASSERT_EQ(expected_scale, getDecimalScale(*result_type, 0)) << "at line " << line;
        }

        Field result_field;

        for (size_t i = 0; i < size; ++i)
        {
            result_column->get(i, result_field);

            auto expected = expected_data[i];

            if (expected.has_value())
            {
                ASSERT_FALSE(result_field.isNull()) << "at line " << line << ", expect not null, at index " << i;

                auto got = result_field.safeGet<ResultFieldType>();

                ASSERT_EQ(expected.value(), got) << "at line " << line << ", at index " << i;
            }
            else
            {
                ASSERT_TRUE(result_field.isNull()) << "at line " << line << ", expect null, at index " << i;
            }
        }
    }
};

TEST_F(TestBinaryArithmeticFunctions, TiDBDivideDecimal)
try
{
    const String func_name = "tidbDivide";

    DataTypePtr decimal_type_1 = std::make_shared<DataTypeDecimal<Decimal128>>(20, 2);
    DataTypePtr decimal_type_2 = std::make_shared<DataTypeDecimal<Decimal128>>(20, 0);
    DataTypePtr nullable_decimal_type_1 = makeNullable(decimal_type_1);
    DataTypePtr nullable_decimal_type_2 = makeNullable(decimal_type_2);
    DataTypePtr null_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());

    std::vector<DataTypePtr> col1_types;
    std::vector<DataTypePtr> col2_types;
    col1_types.push_back(decimal_type_1);
    col1_types.push_back(nullable_decimal_type_1);
    col1_types.push_back(null_type);
    col2_types.push_back(decimal_type_2);
    col2_types.push_back(nullable_decimal_type_2);
    col2_types.push_back(null_type);

    std::vector<Field> null_or_zero_field;
    null_or_zero_field.push_back(Null());
    null_or_zero_field.push_back(Field(DecimalField<Decimal128>(0, 0)));

    std::vector<Int64> values{10, 2, 20, 8, 10, 0, 30, 8, 16, 4};

    const size_t size = 10;

    std::vector<UInt8> col1_null_map{0, 1, 0, 1, 0, 1, 1, 0, 1, 0};
    ASSERT_TRUE(size == col1_null_map.size());

    std::vector<UInt8> col2_null_map{1, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    ASSERT_TRUE(size == col2_null_map.size());

    /// case 1, constant / constant
    /// 1.1 null / null_or_zero
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            for (auto & col2_value : null_or_zero_field)
            {
                if (!col1_type->isNullable())
                    continue;
                if (col2_value.isNull() && !col2_type->isNullable())
                    continue;
                if (!col2_value.isNull() && col2_type->onlyNull())
                    continue;
                auto c1 = col1_type->createColumnConst(size, Null());
                auto c2 = col2_type->createColumnConst(size, col2_value);
                auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
                auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");
                Block block;
                executeFunction(block, col1, col2, func_name);
                checkNullConstantResult(block, size);
            }
        }
    }

    /// 1.2 null / non_null_or_zero
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (!col1_type->isNullable())
                continue;
            if (col2_type->onlyNull())
                continue;
            auto c1 = nullable_decimal_type_1->createColumnConst(size, Null());
            auto c2 = col2_type->createColumnConst(size, Field(DecimalField<Decimal128>(2, 0)));
            Block block;
            auto col1 = ColumnWithTypeAndName(std::move(c1), nullable_decimal_type_1, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");
            executeFunction(block, col1, col2, func_name);
            checkNullConstantResult(block, size);
        }
    }

    /// 1.3 non_null / null_or_zero
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            for (auto & col2_value : null_or_zero_field)
            {
                if (col1_type->onlyNull())
                    continue;
                if (col2_value.isNull() && !col2_type->isNullable())
                    continue;
                if (!col2_value.isNull() && col2_type->onlyNull())
                    continue;
                auto c1 = col1_type->createColumnConst(size, Field(DecimalField<Decimal128>(100, 2)));
                auto c2 = col2_type->createColumnConst(size, col2_value);
                Block block;
                auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
                auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");
                executeFunction(block, col1, col2, func_name);
                checkNullConstantResult(block, size);
            }
        }
    }

    /// 1.4 non_null / non_null_or_zero
    DecimalField<Decimal128> ref(5, 0);
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (col1_type->onlyNull() || col2_type->onlyNull())
                continue;
            auto c1 = col1_type->createColumnConst(size, Field(DecimalField<Decimal128>(1000, 2)));
            auto c2 = col2_type->createColumnConst(size, Field(DecimalField<Decimal128>(2, 0)));
            Block block;
            auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");
            executeFunction(block, col1, col2, func_name);
            const IColumn * res_col = block.getByPosition(2).column.get();
            ASSERT_TRUE(size == res_col->size());
            Field res_field;
            for (size_t i = 0; i < size; i++)
            {
                res_col->get(i, res_field);
                ASSERT_TRUE(!res_field.isNull());
                ASSERT_TRUE(res_field.safeGet<DecimalField<Decimal128>>() == ref);
            }
        }
    }
    /// case 2, vector / constant
    /// 2.1 vector / null_or_zero
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            for (auto & col2_value : null_or_zero_field)
            {
                if (col1_type->onlyNull())
                    continue;
                if (col2_value.isNull() && !col2_type->isNullable())
                    continue;
                if (!col2_value.isNull() && col2_type->onlyNull())
                    continue;
                auto c1_mutable = col1_type->createColumn();
                for (size_t i = 0; i < values.size(); i++)
                {
                    if (col1_type->isNullable() && col1_null_map[i])
                        c1_mutable->insert(Null());
                    else
                        c1_mutable->insert(Field(DecimalField<Decimal128>(values[i], 2)));
                }
                auto c2 = col2_type->createColumnConst(values.size(), col2_value);

                Block block;
                auto col1 = ColumnWithTypeAndName(std::move(c1_mutable), col1_type, "col1");
                auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

                executeFunction(block, col1, col2, func_name);
                checkNullConstantResult(block, values.size());
            }
        }
    }
    /// 2.1 vector / non_null_or_zero
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (col1_type->onlyNull() || col2_type->onlyNull())
                continue;
            auto c1_mutable = col1_type->createColumn();
            for (size_t i = 0; i < values.size(); i++)
            {
                if (col1_type->isNullable() && col1_null_map[i])
                    c1_mutable->insert(Null());
                else
                    c1_mutable->insert(Field(DecimalField<Decimal128>(values[i], 2)));
            }
            auto c2 = col2_type->createColumnConst(values.size(), Field(DecimalField<Decimal128>(2, 0)));

            Block block;
            auto col1 = ColumnWithTypeAndName(std::move(c1_mutable), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

            executeFunction(block, col1, col2, func_name);
            const IColumn * res_col = block.getByPosition(2).column.get();
            ASSERT_TRUE(size == res_col->size());
            Field res_field;
            for (size_t i = 0; i < size; i++)
            {
                DecimalField<Decimal128> result(values[i] / 2, 2);
                res_col->get(i, res_field);
                if (col1_type->isNullable() && col1_null_map[i])
                    ASSERT_TRUE(res_field.isNull());
                else
                {
                    ASSERT_TRUE(!res_field.isNull());
                    ASSERT_TRUE(res_field.safeGet<DecimalField<Decimal128>>() == result);
                }
            }
        }
    }
    /// 3 constant / vector
    /// 3.1 null / vector
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (!col1_type->isNullable())
                continue;
            if (col2_type->onlyNull())
                continue;
            auto c1 = col1_type->createColumnConst(size, Null());
            auto c2 = col2_type->createColumn();
            for (size_t i = 0; i < values.size(); i++)
            {
                if (col2_type->isNullable() && col2_null_map[i])
                    c2->insert(Null());
                else
                    c2->insert(Field(DecimalField<Decimal128>(values[i], 2)));
            }
            Block block;
            auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

            executeFunction(block, col1, col2, func_name);
            checkNullConstantResult(block, size);
        }
    }
    /// 3.2 non_null / vector
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (col1_type->onlyNull() || col2_type->onlyNull())
                continue;
            typename Decimal128::NativeType value = 1;
            for (size_t i = 0; i < values.size(); i++)
            {
                if (values[i] != 0)
                    value *= values[i];
            }
            auto c1 = col1_type->createColumnConst(size, Field(DecimalField<Decimal128>(value, 2)));
            auto c2 = col2_type->createColumn();
            for (size_t i = 0; i < values.size(); i++)
            {
                if (col2_type->isNullable() && col2_null_map[i])
                    c2->insert(Null());
                else
                    c2->insert(Field(DecimalField<Decimal128>(values[i], 0)));
            }
            Block block;
            auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

            executeFunction(block, col1, col2, func_name);
            const IColumn * res_col = block.getByPosition(2).column.get();
            ASSERT_TRUE(size == res_col->size());
            Field res_field;
            for (size_t i = 0; i < size; i++)
            {
                res_col->get(i, res_field);
                if ((col2_type->isNullable() && col2_null_map[i]) || values[i] == 0)
                    ASSERT_TRUE(res_field.isNull());
                else
                {
                    DecimalField<Decimal128> result(value / values[i], 2);
                    ASSERT_TRUE(!res_field.isNull());
                    ASSERT_TRUE(res_field.safeGet<DecimalField<Decimal128>>() == result);
                }
            }
        }
    }
    /// case 4 vector / vector
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (col1_type->onlyNull() || col2_type->onlyNull())
                continue;
            auto c1 = col1_type->createColumn();
            auto c2 = col2_type->createColumn();
            for (size_t i = 0; i < values.size(); i++)
            {
                if (col1_type->isNullable() && col1_null_map[i])
                    c1->insert(Null());
                else
                    c1->insert(Field(DecimalField<Decimal128>(values[i], 2)));
                if (col2_type->isNullable() && col2_null_map[i])
                    c2->insert(Null());
                else
                    c2->insert(Field(DecimalField<Decimal128>(values[i], 0)));
            }
            Block block;
            auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

            executeFunction(block, col1, col2, func_name);
            const IColumn * res_col = block.getByPosition(2).column.get();
            ASSERT_TRUE(size == res_col->size());
            Field res_field;
            for (size_t i = 0; i < size; i++)
            {
                res_col->get(i, res_field);
                if ((col1_type->isNullable() && col1_null_map[i]) || (col2_type->isNullable() && col2_null_map[i]) || values[i] == 0)
                    ASSERT_TRUE(res_field.isNull());
                else
                {
                    DecimalField<Decimal128> result(1, 2);
                    ASSERT_TRUE(!res_field.isNull());
                    ASSERT_TRUE(res_field.safeGet<DecimalField<Decimal128>>() == result);
                }
            }
        }
    }
}
CATCH

TEST_F(TestBinaryArithmeticFunctions, TiDBDivideDouble)
try
{
    const String func_name = "tidbDivide";

    DataTypePtr double_type = std::make_shared<DataTypeFloat64>();
    DataTypePtr nullable_double_type = makeNullable(double_type);
    DataTypePtr null_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());

    std::vector<DataTypePtr> col1_types;
    std::vector<DataTypePtr> col2_types;
    col1_types.push_back(double_type);
    col1_types.push_back(nullable_double_type);
    col1_types.push_back(null_type);
    col2_types.push_back(double_type);
    col2_types.push_back(nullable_double_type);
    col2_types.push_back(null_type);

    std::vector<Field> null_or_zero_field;
    null_or_zero_field.push_back(Null());
    null_or_zero_field.push_back(Field((Float64)0));


    std::vector<Int64> values{10, 2, 20, 8, 10, 0, 30, 8, 16, 4};

    const size_t size = 10;

    std::vector<UInt8> col1_null_map{0, 1, 0, 1, 0, 1, 1, 0, 1, 0};
    ASSERT_TRUE(size == col1_null_map.size());

    std::vector<UInt8> col2_null_map{1, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    ASSERT_TRUE(size == col2_null_map.size());

    /// case 1, constant / constant
    /// 1.1 null / null_or_zero
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            for (auto & col2_value : null_or_zero_field)
            {
                if (!col1_type->isNullable())
                    continue;
                if (col2_value.isNull() && !col2_type->isNullable())
                    continue;
                if (!col2_value.isNull() && col2_type->onlyNull())
                    continue;
                auto c1 = col1_type->createColumnConst(size, Null());
                auto c2 = col2_type->createColumnConst(size, col2_value);
                auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
                auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");
                Block block;
                executeFunction(block, col1, col2, func_name);
                checkNullConstantResult(block, size);
            }
        }
    }

    /// 1.2 null / non_null_or_zero
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (!col1_type->isNullable())
                continue;
            if (col2_type->onlyNull())
                continue;
            auto c1 = col1_type->createColumnConst(size, Null());
            auto c2 = col2_type->createColumnConst(size, Field((Float64)2));
            Block block;
            auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");
            executeFunction(block, col1, col2, func_name);
            checkNullConstantResult(block, size);
        }
    }

    /// 1.3 non_null / null_or_zero
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            for (auto & col2_value : null_or_zero_field)
            {
                if (col1_type->onlyNull())
                    continue;
                if (col2_value.isNull() && !col2_type->isNullable())
                    continue;
                if (!col2_value.isNull() && col2_type->onlyNull())
                    continue;
                auto c1 = col1_type->createColumnConst(size, Field((Float64)1));
                auto c2 = col2_type->createColumnConst(size, col2_value);
                Block block;
                auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
                auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");
                executeFunction(block, col1, col2, func_name);
                checkNullConstantResult(block, size);
            }
        }
    }

    /// 1.4 non_null / non_null_or_zero
    Float64 ref = 5;
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (col1_type->onlyNull() || col2_type->onlyNull())
                continue;
            auto c1 = col1_type->createColumnConst(size, Field((Float64)10));
            auto c2 = col2_type->createColumnConst(size, Field((Float64)2));
            Block block;
            auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");
            executeFunction(block, col1, col2, func_name);
            const IColumn * res_col = block.getByPosition(2).column.get();
            ASSERT_TRUE(size == res_col->size());
            Field res_field;
            for (size_t i = 0; i < size; i++)
            {
                res_col->get(i, res_field);
                ASSERT_TRUE(!res_field.isNull());
                ASSERT_TRUE(res_field.safeGet<Float64>() == ref);
            }
        }
    }
    /// case 2, vector / constant
    /// 2.1 vector / null_or_zero
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            for (auto & col2_value : null_or_zero_field)
            {
                if (col1_type->onlyNull())
                    continue;
                if (col2_value.isNull() && !col2_type->isNullable())
                    continue;
                if (!col2_value.isNull() && col2_type->onlyNull())
                    continue;
                auto c1_mutable = col1_type->createColumn();
                for (size_t i = 0; i < values.size(); i++)
                {
                    if (col1_type->isNullable() && col1_null_map[i])
                        c1_mutable->insert(Null());
                    else
                        c1_mutable->insert(Field((Float64)values[i]));
                }
                auto c2 = col2_type->createColumnConst(values.size(), col2_value);

                Block block;
                auto col1 = ColumnWithTypeAndName(std::move(c1_mutable), col1_type, "col1");
                auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

                executeFunction(block, col1, col2, func_name);
                checkNullConstantResult(block, values.size());
            }
        }
    }
    /// 2.1 vector / non_null_or_zero
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (col1_type->onlyNull() || col2_type->onlyNull())
                continue;
            auto c1_mutable = col1_type->createColumn();
            for (size_t i = 0; i < values.size(); i++)
            {
                if (col1_type->isNullable() && col1_null_map[i])
                    c1_mutable->insert(Null());
                else
                    c1_mutable->insert(Field((Float64)values[i]));
            }
            auto c2 = col2_type->createColumnConst(values.size(), Field((Float64)2));

            Block block;
            auto col1 = ColumnWithTypeAndName(std::move(c1_mutable), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

            executeFunction(block, col1, col2, func_name);
            const IColumn * res_col = block.getByPosition(2).column.get();
            ASSERT_TRUE(size == res_col->size());
            Field res_field;
            for (size_t i = 0; i < size; i++)
            {
                res_col->get(i, res_field);
                if (col1_type->isNullable() && col1_null_map[i])
                    ASSERT_TRUE(res_field.isNull());
                else
                {
                    ASSERT_TRUE(!res_field.isNull());
                    ASSERT_TRUE(res_field.safeGet<Float64>() == values[i] / 2.0);
                }
            }
        }
    }
    /// 3 constant / vector
    /// 3.1 null / vector
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (!col1_type->isNullable())
                continue;
            if (col2_type->onlyNull())
                continue;
            auto c1 = col1_type->createColumnConst(size, Null());
            auto c2 = col2_type->createColumn();
            for (size_t i = 0; i < values.size(); i++)
            {
                if (col2_type->isNullable() && col2_null_map[i])
                    c2->insert(Null());
                else
                    c2->insert(Field((Float64)values[i]));
            }
            Block block;
            auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

            executeFunction(block, col1, col2, func_name);
            checkNullConstantResult(block, size);
        }
    }
    /// 3.2 non_null / vector
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (col1_type->onlyNull() || col2_type->onlyNull())
                continue;
            Float64 value = 1;
            for (size_t i = 0; i < values.size(); i++)
            {
                if (values[i] != 0)
                    value *= values[i];
            }
            auto c1 = col1_type->createColumnConst(size, Field((Float64)value));
            auto c2 = col2_type->createColumn();
            for (size_t i = 0; i < values.size(); i++)
            {
                if (col2_type->isNullable() && col2_null_map[i])
                    c2->insert(Null());
                else
                    c2->insert(Field((Float64)values[i]));
            }
            Block block;
            auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

            executeFunction(block, col1, col2, func_name);
            const IColumn * res_col = block.getByPosition(2).column.get();
            ASSERT_TRUE(size == res_col->size());
            Field res_field;
            for (size_t i = 0; i < size; i++)
            {
                res_col->get(i, res_field);
                if ((col2_type->isNullable() && col2_null_map[i]) || values[i] == 0)
                    ASSERT_TRUE(res_field.isNull());
                else
                {
                    ASSERT_TRUE(!res_field.isNull());
                    ASSERT_TRUE(res_field.safeGet<Float64>() == value / values[i]);
                }
            }
        }
    }
    /// case 4 vector / vector
    for (auto & col1_type : col1_types)
    {
        for (auto & col2_type : col2_types)
        {
            if (col1_type->onlyNull() || col2_type->onlyNull())
                continue;
            auto c1 = col1_type->createColumn();
            auto c2 = col2_type->createColumn();
            for (size_t i = 0; i < values.size(); i++)
            {
                if (col1_type->isNullable() && col1_null_map[i])
                    c1->insert(Null());
                else
                    c1->insert(Field((Float64)values[i]));

                if (col2_type->isNullable() && col2_null_map[i])
                    c2->insert(Null());
                else
                    c2->insert(Field((Float64)values[i]));
            }
            Block block;
            auto col1 = ColumnWithTypeAndName(std::move(c1), col1_type, "col1");
            auto col2 = ColumnWithTypeAndName(std::move(c2), col2_type, "col2");

            executeFunction(block, col1, col2, func_name);
            const IColumn * res_col = block.getByPosition(2).column.get();
            ASSERT_TRUE(size == res_col->size());
            Field res_field;
            for (size_t i = 0; i < size; i++)
            {
                res_col->get(i, res_field);
                if ((col1_type->isNullable() && col1_null_map[i]) || (col2_type->isNullable() && col2_null_map[i]) || values[i] == 0)
                    ASSERT_TRUE(res_field.isNull());
                else
                {
                    ASSERT_TRUE(!res_field.isNull());
                    ASSERT_TRUE(res_field.safeGet<Float64>() == 1);
                }
            }
        }
    }
}
CATCH

TEST_F(TestBinaryArithmeticFunctions, Modulo)
try
{
    const String func_name = "modulo";

    using uint64_limits = std::numeric_limits<UInt64>;
    using int64_limits = std::numeric_limits<Int64>;

    // "{}" is similar to std::nullopt.

    // integer modulo

    executeFunctionWithData<UInt64, UInt64, UInt64>(__LINE__, func_name,
        makeDataType<DataTypeUInt64>(), makeDataType<DataTypeUInt64>(),
        {5, 3, uint64_limits::max(), 1, 0, 0, {}, 0, {}},
        {3, 5, uint64_limits::max() - 1, 0, 1, 0, 0, {}, {}},
        {2, 3, 1, {}, 0, {}, {}, {}, {}});
    executeFunctionWithData<UInt64, Int64, UInt64>(__LINE__, func_name,
        makeDataType<DataTypeUInt64>(), makeDataType<DataTypeInt64>(),
        {5, 5, uint64_limits::max(), uint64_limits::max(), uint64_limits::max(), 1, 0, 0, {}, 0, {}},
        {3, -3, int64_limits::max(), int64_limits::max() - 1, int64_limits::min(), 0, 1, 0, 0, {}, {}},
        {2, 2, 1, 3, int64_limits::max(), {}, 0, {}, {}, {}, {}});
    executeFunctionWithData<Int64, UInt64, Int64>(__LINE__, func_name,
        makeDataType<DataTypeInt64>(), makeDataType<DataTypeUInt64>(),
        {5, -5, int64_limits::max(), int64_limits::min(), 1, 0, 0, {}, 0, {}},
        {3, 3, 998244353, 998244353, 0, 1, 0, 0, {}, {}},
        {2, -2, 466025954, -466025955, {}, 0, {}, {}, {}, {}});
    executeFunctionWithData<Int64, Int64, Int64>(__LINE__, func_name,
        makeDataType<DataTypeInt64>(), makeDataType<DataTypeInt64>(),
        {5, -5, 5, -5, int64_limits::max(), int64_limits::min(), 1, 0, 0, {}, 0, {}},
        {3, 3, -3, -3, int64_limits::min(), int64_limits::max(), 0, 1, 0, 0, {}, {}},
        {2, -2, 2, -2, int64_limits::max(), -1, {}, 0, {}, {}, {}, {}});

    // decimal modulo

    executeFunctionWithData<DecimalField32, DecimalField32, DecimalField32>(__LINE__, func_name,
        makeDataType<Decimal32>(7, 3), makeDataType<Decimal32>(7, 3),
        {
            DecimalField32(3300, 3), DecimalField32(-3300, 3), DecimalField32(3300, 3),
            DecimalField32(-3300, 3), DecimalField32(1000, 3), {}, DecimalField32(0,3), {}
        },
        {
            DecimalField32(1300, 3), DecimalField32(1300, 3), DecimalField32(-1300, 3),
            DecimalField32(-1300, 3), DecimalField32(0, 3), DecimalField32(0, 3), {}, {}
        },
        {
            DecimalField32(700, 3), DecimalField32(-700, 3), DecimalField32(700, 3),
            DecimalField32(-700, 3), {}, {}, {}, {}
        }, 7, 3);

    Int128 large_number_1 = static_cast<Int128>(std::numeric_limits<UInt64>::max()) * 100000;
    executeFunctionWithData<DecimalField128, DecimalField128, DecimalField128>(__LINE__, func_name,
        makeDataType<Decimal128>(38, 5), makeDataType<Decimal128>(38, 5),
        {DecimalField128(large_number_1, 5), DecimalField128(large_number_1, 5), DecimalField128(large_number_1, 5)},
        {DecimalField128(100000, 5), DecimalField128(large_number_1 - 1, 5), DecimalField128(large_number_1 / 2 + 1, 5)},
        {DecimalField128(large_number_1 % 100000, 5), DecimalField128(1, 5), DecimalField128(large_number_1 / 2 - 1, 5)}, 38, 5);

    Int256 large_number_2 = static_cast<Int256>(large_number_1) * large_number_1;
    executeFunctionWithData<DecimalField256, DecimalField256, DecimalField256>(__LINE__, func_name,
        makeDataType<Decimal256>(65, 5), makeDataType<Decimal256>(65, 5),
        {DecimalField256(large_number_2, 5), DecimalField256(large_number_2, 5), DecimalField256(large_number_2, 5)},
        {DecimalField256(static_cast<Int256>(100000), 5), DecimalField256(large_number_2 - 1, 5), DecimalField256(large_number_2 / 2 + 1, 5)},
        {DecimalField256(large_number_2 % 100000, 5), DecimalField256(static_cast<Int256>(1), 5), DecimalField256(large_number_2 / 2 - 1, 5)}, 65, 5);

    // Int64 has a precision of 20, which is larger than Decimal64.
    executeFunctionWithData<DecimalField32, Int64, DecimalField128>(__LINE__, func_name,
        makeDataType<Decimal32>(7, 3), makeDataType<DataTypeInt64>(),
        {DecimalField32(3300, 3), DecimalField32(3300, 3), {}}, {1, 0, {}},
        {DecimalField128(300, 3), {}, {}}, 20, 3);

    executeFunctionWithData<DecimalField32, DecimalField64, DecimalField64>(__LINE__, func_name,
        makeDataType<Decimal32>(7, 5), makeDataType<Decimal64>(15, 3),
        {DecimalField32(3223456, 5)}, {DecimalField64(9244, 3)}, {DecimalField64(450256, 5)}, 15, 5);

    // real modulo

    executeFunctionWithData<Float64, Float64, Float64>(__LINE__, func_name,
        makeDataType<DataTypeFloat64>(), makeDataType<DataTypeFloat64>(),
        {1.3, -1.3, 1.3, -1.3, 3.3, -3.3, 3.3, -3.3, 12.34, 0.0, 0.0, 0.0, {}, {}},
        {1.1, 1.1, -1.1, -1.1, 1.1, 1.1, -1.1, -1.1, 0.0, 12.34, 0.0, {}, 0.0, {}},
        {
            0.19999999999999996, -0.19999999999999996, 0.19999999999999996, -0.19999999999999996,
            1.0999999999999996, -1.0999999999999996, 1.0999999999999996, -1.0999999999999996,
            {}, 0.0, {}, {}, {}, {}
        });
    executeFunctionWithData<Float64, Int64, Float64>(__LINE__, func_name,
        makeDataType<DataTypeFloat64>(), makeDataType<DataTypeInt64>(),
        {1.55, 1.55, {}, 0.0, {}}, {-1, 0, 0, {}, {}}, {0.55, {}, {}, {}, {}});
    executeFunctionWithData<DecimalField32, Float64, Float64>(__LINE__, func_name,
        makeDataType<Decimal32>(7, 3), makeDataType<DataTypeFloat64>(),
        {DecimalField32(1250, 3), DecimalField32(1250, 3), {}, DecimalField32(0, 3), {}},
        {1.0, 0.0, 0.0, {}, {}}, {0.25, {}, {}, {}, {}});

    // const-vector modulo

    executeFunctionWithData<Int64, Int64, Int64>(__LINE__, func_name,
        makeDataType<DataTypeInt64>(), makeDataType<DataTypeInt64>(),
        {3}, {0, 1, 2, 3, 4, 5, 6}, {{}, 0, 1, 0, 3, 3, 3});

    // vector-const modulo

    executeFunctionWithData<Int64, Int64, Int64>(__LINE__, func_name,
        makeDataType<DataTypeInt64>(), makeDataType<DataTypeInt64>(),
        {0, 1, 2, 3}, {0}, {{}, {}, {}, {}});
    executeFunctionWithData<Int64, Int64, Int64>(__LINE__, func_name,
        makeDataType<DataTypeInt64>(), makeDataType<DataTypeInt64>(),
        {0, 1, 2, 3, 4, 5, 6}, {3}, {0, 1, 2, 0, 1, 2, 0});

    // const-const modulo

    executeFunctionWithData<Int64, Int64, Int64>(__LINE__, func_name,
        makeDataType<DataTypeInt64>(), makeDataType<DataTypeInt64>(), {5}, {-3}, {2});
    executeFunctionWithData<Int64, Int64, Int64>(__LINE__, func_name,
        makeDataType<DataTypeInt64>(), makeDataType<DataTypeInt64>(), {0}, {0}, {{}});
    executeFunctionWithData<Int64, Int64, Int64>(__LINE__, func_name,
        makeDataType<DataTypeInt64>(), makeDataType<DataTypeInt64>(), {{}}, {0}, {{}});
    executeFunctionWithData<Int64, Int64, Int64>(__LINE__, func_name,
        makeDataType<DataTypeInt64>(), makeDataType<DataTypeInt64>(), {0}, {{}}, {{}});
    executeFunctionWithData<Int64, Int64, Int64>(__LINE__, func_name,
        makeDataType<DataTypeInt64>(), makeDataType<DataTypeInt64>(), {{}}, {{}}, {{}});
}
CATCH

TEST_F(TestBinaryArithmeticFunctions, ModuloExtra)
try
{
    std::unordered_map<String, DataTypePtr> data_type_map =
    {
        {"Int64", makeDataType<DataTypeInt64>()},
        {"UInt64", makeDataType<DataTypeUInt64>()},
        {"Float64", makeDataType<DataTypeFloat64>()},
        {"DecimalField32", makeDataType<DataTypeDecimal32>(9, 3)},
        {"DecimalField64", makeDataType<DataTypeDecimal64>(18, 6)},
        {"DecimalField128", makeDataType<DataTypeDecimal128>(38, 10)},
        {"DecimalField256", makeDataType<DataTypeDecimal256>(65, 20)},
    };

#define MODULO_TESTCASE(Left, Right, Result, precision, left_scale, right_scale, result_scale) \
    executeFunctionWithData<Left, Right, Result>(__LINE__, "modulo", \
        data_type_map[#Left], data_type_map[#Right], \
        {GetValue<Left, left_scale>::Max(), GetValue<Left, left_scale>::Zero(),GetValue<Left, left_scale>::One(), GetValue<Left, left_scale>::Zero(), {}, {}, {}}, \
        {GetValue<Right, right_scale>::Zero(), GetValue<Right, right_scale>::Max(), GetValue<Right, right_scale>::One(), {}, GetValue<Right, right_scale>::Zero(), GetValue<Right, right_scale>::Max(), {}}, \
        {{}, GetValue<Result, result_scale>::Zero(), GetValue<Result, result_scale>::Zero(), {}, {}, {}, {}}, \
        (precision), (result_scale));

    MODULO_TESTCASE(Int64, Int64, Int64, 0, 0, 0, 0);
    MODULO_TESTCASE(Int64, UInt64, Int64, 0, 0, 0, 0);
    MODULO_TESTCASE(Int64, Float64, Float64, 0, 0, 0, 0);
    MODULO_TESTCASE(Int64, DecimalField32, DecimalField128, 20, 0, 3, 3);
    MODULO_TESTCASE(Int64, DecimalField64, DecimalField128, 20, 0, 6, 6);
    MODULO_TESTCASE(Int64, DecimalField128, DecimalField128, 38, 0, 10, 10);
    MODULO_TESTCASE(Int64, DecimalField256, DecimalField256, 65, 0, 20, 20);

    MODULO_TESTCASE(UInt64, Int64, UInt64, 0, 0, 0, 0);
    MODULO_TESTCASE(UInt64, UInt64, UInt64, 0, 0, 0, 0);
    MODULO_TESTCASE(UInt64, Float64, Float64, 0, 0, 0, 0);
    MODULO_TESTCASE(UInt64, DecimalField32, DecimalField128, 20, 0, 3, 3);
    MODULO_TESTCASE(UInt64, DecimalField64, DecimalField128, 20, 0, 6, 6);
    MODULO_TESTCASE(UInt64, DecimalField128, DecimalField128, 38, 0, 10, 10);
    MODULO_TESTCASE(UInt64, DecimalField256, DecimalField256, 65, 0, 20, 20);

    MODULO_TESTCASE(Float64, Int64, Float64, 0, 0, 0, 0);
    MODULO_TESTCASE(Float64, UInt64, Float64, 0, 0, 0, 0);
    MODULO_TESTCASE(Float64, Float64, Float64, 0, 0, 0, 0);
    MODULO_TESTCASE(Float64, DecimalField32, Float64, 0, 0, 3, 0);
    MODULO_TESTCASE(Float64, DecimalField64, Float64, 0, 0, 6, 0);
    MODULO_TESTCASE(Float64, DecimalField128, Float64, 0, 0, 10, 0);
    MODULO_TESTCASE(Float64, DecimalField256, Float64, 0, 0, 20, 0);

    MODULO_TESTCASE(DecimalField32, Int64, DecimalField128, 20, 3, 0, 3);
    MODULO_TESTCASE(DecimalField32, UInt64, DecimalField128, 20, 3, 0, 3);
    MODULO_TESTCASE(DecimalField32, Float64, Float64, 0, 3, 0, 0);
    MODULO_TESTCASE(DecimalField32, DecimalField32, DecimalField32, 9, 3, 3, 3);
    MODULO_TESTCASE(DecimalField32, DecimalField64, DecimalField64, 18, 3, 6, 6);
    MODULO_TESTCASE(DecimalField32, DecimalField128, DecimalField128, 38, 3, 10, 10);
    MODULO_TESTCASE(DecimalField32, DecimalField256, DecimalField256, 65, 3, 20, 20);

    MODULO_TESTCASE(DecimalField64, Int64, DecimalField128, 20, 6, 0, 6);
    MODULO_TESTCASE(DecimalField64, UInt64, DecimalField128, 20, 6, 0, 6);
    MODULO_TESTCASE(DecimalField64, Float64, Float64, 0, 6, 0, 0);
    MODULO_TESTCASE(DecimalField64, DecimalField32, DecimalField64, 18, 6, 3, 6);
    MODULO_TESTCASE(DecimalField64, DecimalField64, DecimalField64, 18, 6, 6, 6);
    MODULO_TESTCASE(DecimalField64, DecimalField128, DecimalField128, 38, 6, 10, 10);
    MODULO_TESTCASE(DecimalField64, DecimalField256, DecimalField256, 65, 6, 20, 20);

    MODULO_TESTCASE(DecimalField128, Int64, DecimalField128, 38, 10, 0, 10);
    MODULO_TESTCASE(DecimalField128, UInt64, DecimalField128, 38, 10, 0, 10);
    MODULO_TESTCASE(DecimalField128, Float64, Float64, 0, 10, 0, 0);
    MODULO_TESTCASE(DecimalField128, DecimalField32, DecimalField128, 38, 10, 3, 10);
    MODULO_TESTCASE(DecimalField128, DecimalField64, DecimalField128, 38, 10, 6, 10);
    MODULO_TESTCASE(DecimalField128, DecimalField128, DecimalField128, 38, 10, 10, 10);
    MODULO_TESTCASE(DecimalField128, DecimalField256, DecimalField256, 65, 10, 20, 20);

    MODULO_TESTCASE(DecimalField256, Int64, DecimalField256, 65, 20, 0, 20);
    MODULO_TESTCASE(DecimalField256, UInt64, DecimalField256, 65, 20, 0, 20);
    MODULO_TESTCASE(DecimalField256, Float64, Float64, 0, 20, 0, 0);
    MODULO_TESTCASE(DecimalField256, DecimalField32, DecimalField256, 65, 20, 3, 20);
    MODULO_TESTCASE(DecimalField256, DecimalField64, DecimalField256, 65, 20, 6, 20);
    MODULO_TESTCASE(DecimalField256, DecimalField128, DecimalField256, 65, 20, 10, 20);
    MODULO_TESTCASE(DecimalField256, DecimalField256, DecimalField256, 65, 20, 20, 20);

#undef MODULO_TESTCASE
}
CATCH


} // namespace tests
} // namespace DB
