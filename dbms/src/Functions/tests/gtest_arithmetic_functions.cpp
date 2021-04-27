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

} // namespace tests
} // namespace DB
