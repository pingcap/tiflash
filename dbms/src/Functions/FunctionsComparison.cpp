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

#include <Columns/ColumnNullable.h>
#include <Common/typeid_cast.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_COLUMN;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

class FunctionTiDBNullEQ : public IFunction
{
public:
    static constexpr auto name = "tidbNullEQ";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTiDBNullEQ>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    void setCollator(const TiDB::TiDBCollatorPtr & collator_) override
    {
        collator = collator_;
        equals_function->setCollator(collator_);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2.",
                getName(),
                arguments.size());

        /// `NULL <=> x` is always true/false (never NULL), even if `NULL` is represented as `Nothing`.
        if (arguments[0]->onlyNull() || arguments[1]->onlyNull())
            return std::make_shared<DataTypeUInt8>();

        /// Use equals to validate that the input types are comparable.
        /// Always return non-nullable UInt8 because `NULL <=> x` is always true/false (not NULL).
        FunctionEquals().getReturnTypeImpl({removeNullable(arguments[0]), removeNullable(arguments[1])});
        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        const auto & left = block.getByPosition(arguments[0]);
        const auto & right = block.getByPosition(arguments[1]);

        ColumnPtr left_col = left.column;
        ColumnPtr right_col = right.column;

        /// Need to materialize const columns since `removeNullable` does not unwrap `ColumnConst(ColumnNullable)`.
        if (ColumnPtr converted = left_col->convertToFullColumnIfConst())
            left_col = converted;
        if (ColumnPtr converted = right_col->convertToFullColumnIfConst())
            right_col = converted;

        const size_t rows = left_col->size();
        if (unlikely(right_col->size() != rows))
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Columns sizes are different in function {}: left {}, right {}.",
                getName(),
                rows,
                right_col->size());

        /// Fast path for always-NULL columns (Nullable(Nothing)).
        /// `NULL <=> x` equals to `isNull(x)`; `NULL <=> NULL` is always 1.
        if (left_col->onlyNull() || right_col->onlyNull())
        {
            if (left_col->onlyNull() && right_col->onlyNull())
            {
                block.getByPosition(result).column = ColumnUInt8::create(rows, 1);
                return;
            }

            const ColumnPtr & other_col = left_col->onlyNull() ? right_col : left_col;
            auto res_col = ColumnUInt8::create();
            auto & res_data = res_col->getData();
            res_data.resize(rows);
            if (other_col->isColumnNullable())
            {
                const auto & other_nullmap = assert_cast<const ColumnNullable &>(*other_col).getNullMapData();
                res_data.assign(other_nullmap.begin(), other_nullmap.end());
            }
            else
            {
                std::fill(res_data.begin(), res_data.end(), 0);
            }
            block.getByPosition(result).column = std::move(res_col);
            return;
        }

        ColumnPtr left_nested_col = left_col;
        const NullMap * left_nullmap = nullptr;
        if (left_col->isColumnNullable())
        {
            const auto & nullable_col = assert_cast<const ColumnNullable &>(*left_col);
            left_nested_col = nullable_col.getNestedColumnPtr();
            left_nullmap = &nullable_col.getNullMapData();
        }

        ColumnPtr right_nested_col = right_col;
        const NullMap * right_nullmap = nullptr;
        if (right_col->isColumnNullable())
        {
            const auto & nullable_col = assert_cast<const ColumnNullable &>(*right_col);
            right_nested_col = nullable_col.getNestedColumnPtr();
            right_nullmap = &nullable_col.getNullMapData();
        }

        /// Execute `equals` on nested columns.
        Block temp_block;
        temp_block.insert({left_nested_col, removeNullable(left.type), "a"});
        temp_block.insert({right_nested_col, removeNullable(right.type), "b"});
        temp_block.insert({nullptr, std::make_shared<DataTypeUInt8>(), "res"});
        DefaultExecutable(equals_function).execute(temp_block, {0, 1}, 2);

        ColumnPtr eq_col = temp_block.getByPosition(2).column;
        if (ColumnPtr converted = eq_col->convertToFullColumnIfConst())
            eq_col = converted;

        const auto * eq_vec_col = checkAndGetColumn<ColumnUInt8>(eq_col.get());
        if (unlikely(eq_vec_col == nullptr))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Unexpected result column type {} for equals inside {}.",
                eq_col->getName(),
                getName());

        auto res_col = ColumnUInt8::create();
        auto & res_data = res_col->getData();
        const auto & eq_data = eq_vec_col->getData();
        res_data.assign(eq_data.begin(), eq_data.end());

        /// Adjust for NULL values:
        /// - both NULL => 1
        /// - one NULL  => 0
        /// - no NULL   => equals result
        if (left_nullmap != nullptr || right_nullmap != nullptr)
        {
            for (size_t i = 0; i < rows; ++i)
            {
                const bool left_is_null = left_nullmap != nullptr && (*left_nullmap)[i];
                const bool right_is_null = right_nullmap != nullptr && (*right_nullmap)[i];
                if (left_is_null && right_is_null)
                    res_data[i] = 1;
                else if (left_is_null || right_is_null)
                    res_data[i] = 0;
            }
        }

        block.getByPosition(result).column = std::move(res_col);
    }

private:
    TiDB::TiDBCollatorPtr collator = nullptr;
    std::shared_ptr<FunctionEquals> equals_function = std::make_shared<FunctionEquals>();
};

void registerFunctionsComparison(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEquals>();
    factory.registerFunction<FunctionNotEquals>();
    factory.registerFunction<FunctionLess>();
    factory.registerFunction<FunctionGreater>();
    factory.registerFunction<FunctionLessOrEquals>();
    factory.registerFunction<FunctionGreaterOrEquals>();
    factory.registerFunction<FunctionStrcmp>();
    factory.registerFunction<FunctionIsTrue>();
    factory.registerFunction<FunctionIsTrueWithNull>();
    factory.registerFunction<FunctionIsFalse>();
    factory.registerFunction<FunctionIsFalseWithNull>();
    factory.registerFunction<FunctionTiDBNullEQ>();
}

template <>
void FunctionComparison<EqualsOp, NameEquals>::executeTupleImpl(
    Block & block,
    size_t result,
    const ColumnsWithTypeAndName & x,
    const ColumnsWithTypeAndName & y,
    size_t tuple_size) const
{
    return executeTupleEqualityImpl<FunctionComparison<EqualsOp, NameEquals>, FunctionAnd>(
        block,
        result,
        x,
        y,
        tuple_size);
}

template <>
void FunctionComparison<NotEqualsOp, NameNotEquals>::executeTupleImpl(
    Block & block,
    size_t result,
    const ColumnsWithTypeAndName & x,
    const ColumnsWithTypeAndName & y,
    size_t tuple_size) const
{
    return executeTupleEqualityImpl<FunctionComparison<NotEqualsOp, NameNotEquals>, FunctionOr>(
        block,
        result,
        x,
        y,
        tuple_size);
}

template <>
void FunctionComparison<LessOp, NameLess>::executeTupleImpl(
    Block & block,
    size_t result,
    const ColumnsWithTypeAndName & x,
    const ColumnsWithTypeAndName & y,
    size_t tuple_size) const
{
    return executeTupleLessGreaterImpl<FunctionComparison<LessOp, NameLess>, FunctionComparison<LessOp, NameLess>>(
        block,
        result,
        x,
        y,
        tuple_size);
}

template <>
void FunctionComparison<GreaterOp, NameGreater>::executeTupleImpl(
    Block & block,
    size_t result,
    const ColumnsWithTypeAndName & x,
    const ColumnsWithTypeAndName & y,
    size_t tuple_size) const
{
    return executeTupleLessGreaterImpl<
        FunctionComparison<GreaterOp, NameGreater>,
        FunctionComparison<GreaterOp, NameGreater>>(block, result, x, y, tuple_size);
}

template <>
void FunctionComparison<LessOrEqualsOp, NameLessOrEquals>::executeTupleImpl(
    Block & block,
    size_t result,
    const ColumnsWithTypeAndName & x,
    const ColumnsWithTypeAndName & y,
    size_t tuple_size) const
{
    return executeTupleLessGreaterImpl<
        FunctionComparison<LessOp, NameLess>,
        FunctionComparison<LessOrEqualsOp, NameLessOrEquals>>(block, result, x, y, tuple_size);
}

template <>
void FunctionComparison<GreaterOrEqualsOp, NameGreaterOrEquals>::executeTupleImpl(
    Block & block,
    size_t result,
    const ColumnsWithTypeAndName & x,
    const ColumnsWithTypeAndName & y,
    size_t tuple_size) const
{
    return executeTupleLessGreaterImpl<
        FunctionComparison<GreaterOp, NameGreater>,
        FunctionComparison<GreaterOrEqualsOp, NameGreaterOrEquals>>(block, result, x, y, tuple_size);
}

template <>
void FunctionComparison<CmpOp, NameStrcmp, StrcmpReturnColumnType>::executeTupleImpl(
    [[maybe_unused]] Block & block,
    [[maybe_unused]] size_t result,
    [[maybe_unused]] const ColumnsWithTypeAndName & x,
    [[maybe_unused]] const ColumnsWithTypeAndName & y,
    [[maybe_unused]] size_t tuple_size) const
{
    throw DB::Exception("Strcmp can not be used with tuple");
}

} // namespace DB
