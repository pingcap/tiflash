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

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/FieldVisitors.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <Core/Row.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Interpreters/NullableUtils.h>
#include <Interpreters/Set.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <TiDB/Decode/TypeMapping.h>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int SET_SIZE_LIMIT_EXCEEDED;
extern const int TYPE_MISMATCH;
extern const int INCORRECT_ELEMENT_OF_SET;
extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
extern const int COP_BAD_DAG_REQUEST;
} // namespace ErrorCodes


template <typename Method>
void NO_INLINE Set::insertFromBlockImpl(
    Method & method,
    const ColumnRawPtrs & key_columns,
    size_t rows,
    SetVariants & variants,
    ConstNullMapPtr null_map)
{
    if (null_map)
        insertFromBlockImplCase<Method, true>(method, key_columns, rows, variants, null_map);
    else
        insertFromBlockImplCase<Method, false>(method, key_columns, rows, variants, null_map);
}


template <typename Method, bool has_null_map>
void NO_INLINE Set::insertFromBlockImplCase(
    Method & method,
    const ColumnRawPtrs & key_columns,
    size_t rows,
    SetVariants & variants,
    ConstNullMapPtr null_map)
{
    typename Method::State state(key_columns, key_sizes, collators);
    std::vector<String> sort_key_containers;
    sort_key_containers.resize(key_columns.size(), "");

    /// For all rows
    for (size_t i = 0; i < rows; ++i)
    {
        if (has_null_map && (*null_map)[i])
            continue;

        auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool, sort_key_containers);

        if (emplace_result.isInserted() && keys_size == 1)
        {
            unique_set_elements->emplace((*key_columns[0])[i]);
        }
    }
}


void Set::setHeader(const Block & block)
{
    std::unique_lock lock(rwlock);

    if (!empty())
        return;

    keys_size = block.columns();
    ColumnRawPtrs key_columns;
    key_columns.reserve(keys_size);
    data_types.reserve(keys_size);

    /// The constant columns to the right of IN are not supported directly. For this, they first materialize.
    Columns materialized_columns;

    /// Remember the columns we will work with
    for (size_t i = 0; i < keys_size; ++i)
    {
        key_columns.emplace_back(block.safeGetByPosition(i).column.get());
        data_types.emplace_back(block.safeGetByPosition(i).type);

        if (ColumnPtr converted = key_columns.back()->convertToFullColumnIfConst())
        {
            materialized_columns.emplace_back(converted);
            key_columns.back() = materialized_columns.back().get();
        }
    }

    /// We will insert to the Set only keys, where all components are not NULL.
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);

    /// Choose data structure to use for the set.
    data.init(data.chooseMethod(key_columns, key_sizes, collators));
}


bool Set::insertFromBlock(const Block & block, bool fill_set_elements)
{
    std::unique_lock lock(rwlock);

    if (empty())
        throw Exception("Method Set::setHeader must be called before Set::insertFromBlock", ErrorCodes::LOGICAL_ERROR);

    ColumnRawPtrs key_columns;
    key_columns.reserve(keys_size);

    /// The constant columns to the right of IN are not supported directly. For this, they first materialize.
    Columns materialized_columns;

    /// Remember the columns we will work with
    for (size_t i = 0; i < keys_size; ++i)
    {
        key_columns.emplace_back(block.safeGetByPosition(i).column.get());

        if (ColumnPtr converted = key_columns.back()->convertToFullColumnIfConst())
        {
            materialized_columns.emplace_back(converted);
            key_columns.back() = materialized_columns.back().get();
        }
    }

    size_t rows = block.rows();

    /// We will insert to the Set only keys, where all components are not NULL.
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);

    switch (data.type)
    {
    case SetVariants::Type::EMPTY:
        break;
#define M(NAME)                                                             \
    case SetVariants::Type::NAME:                                           \
        insertFromBlockImpl(*data.NAME, key_columns, rows, data, null_map); \
        break;
        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }

    if (fill_set_elements)
    {
        for (size_t i = 0; i < rows; ++i)
        {
            std::vector<Field> new_set_elements;
            for (size_t j = 0; j < keys_size; ++j)
                new_set_elements.push_back((*key_columns[j])[i]);

            set_elements->emplace_back(std::move(new_set_elements));
        }
    }

    return limits.check(getTotalRowCount(), getTotalByteCount(), "IN-set", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}


static Field extractValueFromNode(ASTPtr & node, const IDataType & type, const Context & context)
{
    if (auto * lit = typeid_cast<ASTLiteral *>(node.get()))
    {
        return convertFieldToType(lit->value, type);
    }
    else if (typeid_cast<ASTFunction *>(node.get()))
    {
        std::pair<Field, DataTypePtr> value_raw = evaluateConstantExpression(node, context);
        return convertFieldToType(value_raw.first, type, value_raw.second.get());
    }
    else
        throw Exception(
            "Incorrect element of set. Must be literal or constant expression.",
            ErrorCodes::INCORRECT_ELEMENT_OF_SET);
}


void Set::createFromAST(const DataTypes & types, ASTPtr node, const Context & context, bool fill_set_elements)
{
    /// Will form a block with values from the set.

    Block header;
    size_t num_columns = types.size();
    for (size_t i = 0; i < num_columns; ++i)
        header.insert(ColumnWithTypeAndName(types[i]->createColumn(), types[i], "_" + toString(i)));
    setHeader(header);

    MutableColumns columns = header.cloneEmptyColumns();

    Row tuple_values;
    ASTExpressionList & list = typeid_cast<ASTExpressionList &>(*node);
    for (auto & elem : list.children)
    {
        if (num_columns == 1)
        {
            Field value = extractValueFromNode(elem, *types[0], context);

            if (!value.isNull())
                columns[0]->insert(value);
            else
                setContainsNullValue(true);
        }
        else if (auto * func = typeid_cast<ASTFunction *>(elem.get()))
        {
            if (func->name != "tuple")
                throw Exception("Incorrect element of set. Must be tuple.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);

            size_t tuple_size = func->arguments->children.size();
            if (tuple_size != num_columns)
                throw Exception(
                    "Incorrect size of tuple in set: " + toString(tuple_size) + " instead of " + toString(num_columns),
                    ErrorCodes::INCORRECT_ELEMENT_OF_SET);

            if (tuple_values.empty())
                tuple_values.resize(tuple_size);

            size_t i = 0;
            for (; i < tuple_size; ++i)
            {
                Field value = extractValueFromNode(func->arguments->children[i], *types[i], context);

                /// If at least one of the elements of the tuple has an impossible (outside the range of the type) value, then the entire tuple too.
                if (value.isNull())
                    break;

                tuple_values[i] = value;
            }

            if (i == tuple_size)
                for (i = 0; i < tuple_size; ++i)
                    columns[i]->insert(tuple_values[i]);
        }
        else
            throw Exception("Incorrect element of set", ErrorCodes::INCORRECT_ELEMENT_OF_SET);
    }

    Block block = header.cloneWithColumns(std::move(columns));
    insertFromBlock(block, fill_set_elements);
}

std::vector<const tipb::Expr *> Set::createFromDAGExpr(
    const DataTypes & types,
    const tipb::Expr & expr,
    bool fill_set_elements,
    const TimezoneInfo & timezone_info)
{
    /// Will form a block with values from the set.

    Block header;
    size_t num_columns = types.size();
    if (num_columns != 1)
    {
        throw Exception(
            "Incorrect element of set, tuple in is not supported yet",
            ErrorCodes::INCORRECT_ELEMENT_OF_SET);
    }
    for (size_t i = 0; i < num_columns; ++i)
        header.insert(ColumnWithTypeAndName(types[i]->createColumn(), types[i], "_" + toString(i)));
    setHeader(header);

    MutableColumns columns = header.cloneEmptyColumns();
    std::vector<const tipb::Expr *> remaining_exprs;

    // if left arg is null constant, just return without decode children expr
    if (types[0]->onlyNull())
        return remaining_exprs;

    for (int i = 1; i < expr.children_size(); i++)
    {
        const auto & child = expr.children(i);
        // todo support constant expression by constant folding
        if (!isLiteralExpr(child))
        {
            remaining_exprs.push_back(&child);
            continue;
        }
        Field value = decodeLiteral(child);

        if (child.field_type().tp() == TiDB::TypeTimestamp && !timezone_info.is_utc_timezone)
        {
            static const auto & time_zone_utc = DateLUT::instance("UTC");
            UInt64 from_time = value.get<UInt64>();
            UInt64 result_time = from_time;

            // It looks weird that converting from UTC to dag timezone can work as expected.
            if (timezone_info.is_name_based)
                convertTimeZone(from_time, result_time, time_zone_utc, *timezone_info.timezone);
            else if (timezone_info.timezone_offset != 0)
                convertTimeZoneByOffset(from_time, result_time, true, timezone_info.timezone_offset);
            value = Field(result_time);
        }

        const auto & type = types[0];
        value = convertFieldToType(value, *type);

        if (!value.isNull())
            columns[0]->insert(value);
        else
            setContainsNullValue(true);
    }

    Block block = header.cloneWithColumns(std::move(columns));
    insertFromBlock(block, fill_set_elements);
    return remaining_exprs;
}

ColumnPtr Set::execute(const Block & block, bool negative) const
{
    size_t num_key_columns = block.columns();

    if (0 == num_key_columns)
        throw Exception("Logical error: no columns passed to Set::execute method.", ErrorCodes::LOGICAL_ERROR);

    auto res = ColumnUInt8::create();
    ColumnUInt8::Container & vec_res = res->getData();
    vec_res.resize(block.safeGetByPosition(0).column->size());

    std::shared_lock lock(rwlock);

    /// If the set is empty.
    if (data_types.empty())
    {
        if (negative)
            memset(&vec_res[0], 1, vec_res.size());
        else
            memset(&vec_res[0], 0, vec_res.size());
        return res;
    }

    if (data_types.size() != num_key_columns)
    {
        std::stringstream message;
        message << "Number of columns in section IN doesn't match. " << num_key_columns << " at left, "
                << data_types.size() << " at right.";
        throw Exception(message.str(), ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);
    }

    /// Remember the columns we will work with. Also check that the data types are correct.
    ColumnRawPtrs key_columns;
    key_columns.reserve(num_key_columns);

    /// The constant columns to the left of IN are not supported directly. For this, they first materialize.
    Columns materialized_columns;

    for (size_t i = 0; i < num_key_columns; ++i)
    {
        key_columns.push_back(block.safeGetByPosition(i).column.get());

        if (!removeNullable(data_types[i])->equals(*removeNullable(block.safeGetByPosition(i).type)))
            throw Exception(
                "Types of column " + toString(i + 1) + " in section IN don't match: " + data_types[i]->getName()
                    + " on the right, " + block.safeGetByPosition(i).type->getName() + " on the left.",
                ErrorCodes::TYPE_MISMATCH);

        if (ColumnPtr converted = key_columns.back()->convertToFullColumnIfConst())
        {
            materialized_columns.emplace_back(converted);
            key_columns.back() = materialized_columns.back().get();
        }
    }

    /// We will check existence in Set only for keys, where all components are not NULL.
    ColumnPtr null_map_holder;
    ConstNullMapPtr null_map{};
    extractNestedColumnsAndNullMap(key_columns, null_map_holder, null_map);

    executeOrdinary(key_columns, vec_res, negative, null_map);

    return res;
}


template <typename Method>
void NO_INLINE Set::executeImpl(
    Method & method,
    const ColumnRawPtrs & key_columns,
    ColumnUInt8::Container & vec_res,
    bool negative,
    size_t rows,
    ConstNullMapPtr null_map) const
{
    if (null_map)
        executeImplCase<Method, true>(method, key_columns, vec_res, negative, rows, null_map);
    else
        executeImplCase<Method, false>(method, key_columns, vec_res, negative, rows, null_map);
}


template <typename Method, bool has_null_map>
void NO_INLINE Set::executeImplCase(
    Method & method,
    const ColumnRawPtrs & key_columns,
    ColumnUInt8::Container & vec_res,
    bool negative,
    size_t rows,
    ConstNullMapPtr null_map) const
{
    Arena pool;
    typename Method::State state(key_columns, key_sizes, collators);
    std::vector<String> sort_key_containers;
    sort_key_containers.resize(key_columns.size(), "");

    /// NOTE Optimization is not used for consecutive identical values.

    /// For all rows
    for (size_t i = 0; i < rows; ++i)
    {
        if (has_null_map && (*null_map)[i])
            vec_res[i] = negative;
        else
        {
            auto find_result = state.findKey(method.data, i, pool, sort_key_containers);
            vec_res[i] = negative ^ find_result.isFound();
        }
    }
}


void Set::executeOrdinary(
    const ColumnRawPtrs & key_columns,
    ColumnUInt8::Container & vec_res,
    bool negative,
    ConstNullMapPtr null_map) const
{
    size_t rows = key_columns[0]->size();

    switch (data.type)
    {
    case SetVariants::Type::EMPTY:
        break;
#define M(NAME)                                                                  \
    case SetVariants::Type::NAME:                                                \
        executeImpl(*data.NAME, key_columns, vec_res, negative, rows, null_map); \
        break;
        APPLY_FOR_SET_VARIANTS(M)
#undef M
    }
}


} // namespace DB
