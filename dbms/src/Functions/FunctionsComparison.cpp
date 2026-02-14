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

#include <Columns/ColumnConst.h>
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
            if (other_col->isColumnNullable())
            {
                const auto & other_nullmap = assert_cast<const ColumnNullable &>(*other_col).getNullMapData();
                auto res_col = ColumnUInt8::create();
                auto & res_data = res_col->getData();
                res_data.assign(other_nullmap.begin(), other_nullmap.end());
                block.getByPosition(result).column = std::move(res_col);
            }
            else
            {
                block.getByPosition(result).column = ColumnUInt8::create(rows, 0);
            }
            return;
        }

        auto unwrapNullableColumn = [rows](const ColumnPtr & col, ColumnPtr & nested_col, const NullMap *& nullmap) {
            nested_col = col;
            nullmap = nullptr;

            if (const auto * const_col = typeid_cast<const ColumnConst *>(col.get()))
            {
                const auto & data_col = const_col->getDataColumn();
                if (data_col.isColumnNullable())
                {
                    /// `ColumnConst(ColumnNullable(NULL))` is handled by the `onlyNull()` fast path above.
                    /// If we reach here, the nullable constant must be non-NULL, so there is no nullmap to apply.
                    const auto & nullable_col = assert_cast<const ColumnNullable &>(data_col);
                    nested_col = ColumnConst::create(nullable_col.getNestedColumnPtr(), rows);
                }
                return;
            }

            if (col->isColumnNullable())
            {
                const auto & nullable_col = assert_cast<const ColumnNullable &>(*col);
                nested_col = nullable_col.getNestedColumnPtr();
                nullmap = &nullable_col.getNullMapData();
            }
        };

        ColumnPtr left_nested_col = left_col;
        const NullMap * left_nullmap = nullptr;
        unwrapNullableColumn(left_col, left_nested_col, left_nullmap);

        ColumnPtr right_nested_col = right_col;
        const NullMap * right_nullmap = nullptr;
        unwrapNullableColumn(right_col, right_nested_col, right_nullmap);

        /// Execute `equals` on nested columns.
        Block temp_block;
        temp_block.insert({left_nested_col, removeNullable(left.type), "a"});
        temp_block.insert({right_nested_col, removeNullable(right.type), "b"});
        temp_block.insert({nullptr, std::make_shared<DataTypeUInt8>(), "res"});
        DefaultExecutable(equals_function).execute(temp_block, {0, 1}, 2);

        ColumnPtr eq_col = temp_block.getByPosition(2).column;
        if (left_nullmap == nullptr && right_nullmap == nullptr)
        {
            block.getByPosition(result).column = std::move(eq_col);
            return;
        }

        if (ColumnPtr converted = eq_col->convertToFullColumnIfConst())
            eq_col = converted;

        /// Adjust for NULL values:
        /// - both NULL => 1
        /// - one NULL  => 0
        /// - no NULL   => equals result
        auto eq_mutable = (*std::move(eq_col)).mutate();
        auto * eq_vec_col = typeid_cast<ColumnUInt8 *>(eq_mutable.get());
        if (unlikely(eq_vec_col == nullptr))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Unexpected result column type {} for equals inside {}.",
                eq_mutable->getName(),
                getName());

        auto & res_data = eq_vec_col->getData();
        if (left_nullmap != nullptr && right_nullmap != nullptr)
        {
            const auto & left_data = *left_nullmap;
            const auto & right_data = *right_nullmap;
            for (size_t i = 0; i < rows; ++i)
            {
                const UInt8 left_is_null = left_data[i] != 0;
                const UInt8 right_is_null = right_data[i] != 0;

                const UInt8 any_null = left_is_null | right_is_null;
                const UInt8 both_null = left_is_null & right_is_null;

                /// Keep equals result when `any_null == 0`, otherwise override it to 0.
                /// Finally, override to 1 when `both_null == 1`.
                const auto eq = static_cast<UInt8>(res_data[i] != 0);
                res_data[i] = (eq & static_cast<UInt8>(!any_null)) | both_null;
            }
        }
        else if (left_nullmap != nullptr)
        {
            const auto & left_data = *left_nullmap;
            for (size_t i = 0; i < rows; ++i)
            {
                const UInt8 left_is_null = left_data[i] != 0;
                const auto eq = static_cast<UInt8>(res_data[i] != 0);
                res_data[i] = eq & static_cast<UInt8>(!left_is_null);
            }
        }
        else if (right_nullmap != nullptr)
        {
            const auto & right_data = *right_nullmap;
            for (size_t i = 0; i < rows; ++i)
            {
                const UInt8 right_is_null = right_data[i] != 0;
                const auto eq = static_cast<UInt8>(res_data[i] != 0);
                res_data[i] = eq & static_cast<UInt8>(!right_is_null);
            }
        }

        block.getByPosition(result).column = std::move(eq_mutable);
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
