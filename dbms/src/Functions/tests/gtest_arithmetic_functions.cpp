#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <Functions/FunctionFactory.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Context.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

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

<<<<<<< HEAD
=======
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

    // decimal overflow test.

    // for example, 999'999'999 % 1.0000'0000 can check whether Decimal32 is doing arithmetic on Int64.
    // scaling 999'999'999 (Decimal(9, 0)) to Decimal(9, 8) needs to multiply it with 1'0000'0000.
    // if it uses Int32, it will overflow and get wrong result (something like 0.69325056).

#define MODULO_OVERFLOW_TESTCASE(Decimal, DecimalField, precision) \
    { \
        auto & builder = DecimalMaxValue::instance(); \
        auto max_scale = std::min(decimal_max_scale, static_cast<ScaleType>(precision) - 1); \
        auto exp10_x = static_cast<Decimal::NativeType>(builder.Get(max_scale)) + 1; /* exp10_x: 10^x */ \
        auto decimal_max = exp10_x * 10 - 1; \
        auto zero = static_cast<Decimal::NativeType>(0); /* for Int256 */ \
        executeFunctionWithData<DecimalField, DecimalField, DecimalField>(__LINE__, func_name, \
            makeDataType<Decimal>((precision), 0), makeDataType<Decimal>((precision), max_scale), \
            {DecimalField(decimal_max, 0)}, {DecimalField(exp10_x, max_scale)}, {DecimalField(zero, max_scale)}, (precision), max_scale); \
        executeFunctionWithData<DecimalField, DecimalField, DecimalField>(__LINE__, func_name, \
            makeDataType<Decimal>((precision), max_scale), makeDataType<Decimal>((precision), 0), \
            {DecimalField(exp10_x, max_scale)}, {DecimalField(decimal_max, 0)}, {DecimalField(exp10_x, max_scale)}, (precision), max_scale); \
    }

    MODULO_OVERFLOW_TESTCASE(Decimal32, DecimalField32, 9);
    MODULO_OVERFLOW_TESTCASE(Decimal64, DecimalField64, 18);
    MODULO_OVERFLOW_TESTCASE(Decimal128, DecimalField128, 38);
    MODULO_OVERFLOW_TESTCASE(Decimal256, DecimalField256, 65);

#undef MODULO_OVERFLOW_TESTCASE

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


>>>>>>> c407b167c... Fix function `mod` decimal scale overflow (#2462)
} // namespace tests
} // namespace DB
