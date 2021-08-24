#pragma once

#include <common/mem_utils.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsCommon.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsHigherOrder.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/sortBlock.h>

#include <array>
#include <map>
#include "AggregateFunctionGroupUniqArray.h"
#include <Interpreters/SetVariants.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/// This class implements a wrapper around an aggregate function. Despite its name,
/// this is an adapter. It is used to handle aggregate functions that are called with
/// at least one nullable argument. It implements the logic according to which any
/// row that contains at least one NULL is skipped.

/// If all rows had NULL, the behaviour is determined by "result_is_nullable" template parameter.
///  true - return NULL; false - return value from empty aggregation state of nested function.

template <bool result_is_nullable, typename Derived>
class AggregateFunctionNullBase : public IAggregateFunctionHelper<Derived>
{
protected:
    AggregateFunctionPtr nested_function;
    size_t prefix_size;

    /** In addition to data for nested aggregate function, we keep a flag
      *  indicating - was there at least one non-NULL value accumulated.
      * In case of no not-NULL values, the function will return NULL.
      *
      * We use prefix_size bytes for flag to satisfy the alignment requirement of nested state.
      */

    AggregateDataPtr nestedPlace(AggregateDataPtr __restrict place) const noexcept
    {
        return place + prefix_size;
    }

    ConstAggregateDataPtr nestedPlace(ConstAggregateDataPtr __restrict place) const noexcept
    {
        return place + prefix_size;
    }

    static void initFlag(AggregateDataPtr __restrict place) noexcept
    {
        if (result_is_nullable)
            place[0] = 0;
    }

    static void setFlag(AggregateDataPtr __restrict place) noexcept
    {
        if (result_is_nullable)
            place[0] = 1;
    }

    static bool getFlag(ConstAggregateDataPtr __restrict place) noexcept
    {
        return result_is_nullable ? place[0] : 1;
    }

public:
    AggregateFunctionNullBase(AggregateFunctionPtr nested_function_)
        : nested_function{nested_function_}
    {
        if (result_is_nullable)
            prefix_size = nested_function->alignOfData();
        else
            prefix_size = 0;
    }

    String getName() const override
    {
        /// This is just a wrapper. The function for Nullable arguments is named the same as the nested function itself.
        return nested_function->getName();
    }

    void setCollators(TiDB::TiDBCollators & collators) override
    {
        nested_function->setCollators(collators);
    }

    DataTypePtr getReturnType() const override
    {
        return result_is_nullable
            ? makeNullable(nested_function->getReturnType())
            : nested_function->getReturnType();
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        initFlag(place);
        nested_function->create(nestedPlace(place));
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        nested_function->destroy(nestedPlace(place));
    }

    bool hasTrivialDestructor() const override
    {
        return nested_function->hasTrivialDestructor();
    }

    size_t sizeOfData() const override
    {
        return prefix_size + nested_function->sizeOfData();
    }

    size_t alignOfData() const override
    {
        return nested_function->alignOfData();
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        if (result_is_nullable && getFlag(rhs))
            setFlag(place);

        nested_function->merge(nestedPlace(place), nestedPlace(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        bool flag = getFlag(place);
        if (result_is_nullable)
            writeBinary(flag, buf);
        if (flag)
            nested_function->serialize(nestedPlace(place), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
    {
        bool flag = 1;
        if (result_is_nullable)
            readBinary(flag, buf);
        if (flag)
        {
            setFlag(place);
            nested_function->deserialize(nestedPlace(place), buf, arena);
        }
    }

    void insertResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        if constexpr (result_is_nullable)
        {
            ColumnNullable & to_concrete = static_cast<ColumnNullable &>(to);
            if (getFlag(place))
            {
                nested_function->insertResultInto(nestedPlace(place), to_concrete.getNestedColumn(), arena);
                to_concrete.getNullMapData().push_back(0);
            }
            else
            {
                to_concrete.insertDefault();
            }
        }
        else
        {
            nested_function->insertResultInto(nestedPlace(place), to, arena);
        }
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_function->allocatesMemoryInArena();
    }

    bool isState() const override
    {
        return nested_function->isState();
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

template <bool result_is_nullable, bool input_is_nullable>
class AggregateFunctionFirstRowNull : public IAggregateFunctionHelper<AggregateFunctionFirstRowNull<result_is_nullable, input_is_nullable>>
{
protected:
    AggregateFunctionPtr nested_function;
    size_t prefix_size;

    /** In addition to data for nested aggregate function, we keep a flag
      *  indicating - was there at least one non-NULL value accumulated.
      * In case of no not-NULL values, the function will return NULL.
      *
      * We use prefix_size bytes for flag to satisfy the alignment requirement of nested state.
      */

    AggregateDataPtr nestedPlace(AggregateDataPtr __restrict place) const noexcept
    {
        return place + prefix_size;
    }

    ConstAggregateDataPtr nestedPlace(ConstAggregateDataPtr __restrict place) const noexcept
    {
        return place + prefix_size;
    }

    static void initFlag(AggregateDataPtr __restrict place) noexcept
    {
        if (result_is_nullable)
            place[0] = 0;
    }

    static void setFlag(AggregateDataPtr __restrict place, UInt8 status) noexcept
    {
        if (result_is_nullable)
            place[0] = status;
    }

    /// 0 means there is no input yet
    /// 1 meas there is a not-null input
    /// 2 means there is a null input
    static UInt8 getFlag(ConstAggregateDataPtr __restrict place) noexcept
    {
        return result_is_nullable ? place[0] : 1;
    }

public:
    AggregateFunctionFirstRowNull(AggregateFunctionPtr nested_function_)
        : nested_function{nested_function_}
    {
        if (result_is_nullable)
            prefix_size = nested_function->alignOfData();
        else
            prefix_size = 0;
    }

    String getName() const override
    {
        /// This is just a wrapper. The function for Nullable arguments is named the same as the nested function itself.
        return nested_function->getName();
    }

    void setCollators(TiDB::TiDBCollators & collators) override
    {
        nested_function->setCollators(collators);
    }

    DataTypePtr getReturnType() const override
    {
        return result_is_nullable
               ? makeNullable(nested_function->getReturnType())
               : nested_function->getReturnType();
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        initFlag(place);
        nested_function->create(nestedPlace(place));
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        nested_function->destroy(nestedPlace(place));
    }

    bool hasTrivialDestructor() const override
    {
        return nested_function->hasTrivialDestructor();
    }

    size_t sizeOfData() const override
    {
        return prefix_size + nested_function->sizeOfData();
    }

    size_t alignOfData() const override
    {
        return nested_function->alignOfData();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if constexpr (input_is_nullable)
        {
            if (this->getFlag(place) == 0)
            {
                const ColumnNullable * column = static_cast<const ColumnNullable *>(columns[0]);
                bool is_null = column->isNullAt(row_num);
                this->setFlag(place, is_null ? 2 : 1);
                if (!is_null)
                {
                    const IColumn * nested_column = &column->getNestedColumn();
                    this->nested_function->add(this->nestedPlace(place), &nested_column, row_num, arena);
                }
            }
        }
        else
        {
            this->setFlag(place, 1);
            this->nested_function->add(this->nestedPlace(place), columns, row_num, arena);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        if constexpr (result_is_nullable)
        {
            if (getFlag(place) == 0)
            {
                if (getFlag(rhs) > 0)
                {
                    setFlag(place, getFlag(rhs));
                }
                if (getFlag(rhs) == 1)
                    nested_function->merge(nestedPlace(place), nestedPlace(rhs), arena);
            }
        }
        else
        {
            nested_function->merge(nestedPlace(place), nestedPlace(rhs), arena);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        UInt8 flag = getFlag(place);
        if (result_is_nullable)
            writeBinary(flag, buf);
        if (flag == 1)
            nested_function->serialize(nestedPlace(place), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
    {
        UInt8 flag = 1;
        if (result_is_nullable)
            readBinary(flag, buf);
        if (flag == 1)
        {
            setFlag(place, 1);
            nested_function->deserialize(nestedPlace(place), buf, arena);
        }
        else if (flag == 2)
        {
            setFlag(place, 2);
        }
    }

    void insertResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        if constexpr (result_is_nullable)
        {
            ColumnNullable & to_concrete = static_cast<ColumnNullable &>(to);
            UInt8 flag = getFlag(place);
            if (flag == 1)
            {
                nested_function->insertResultInto(nestedPlace(place), to_concrete.getNestedColumn(), arena);
                to_concrete.getNullMapData().push_back(0);
            }
            else
            {
                to_concrete.insertDefault();
            }
        }
        else
        {
            nested_function->insertResultInto(nestedPlace(place), to, arena);
        }
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_function->allocatesMemoryInArena();
    }

    bool isState() const override
    {
        return nested_function->isState();
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

/** There are two cases: for single argument and variadic.
  * Code for single argument is much more efficient.
  */
template <bool result_is_nullable, bool input_is_nullable>
class AggregateFunctionNullUnary final : public AggregateFunctionNullBase<result_is_nullable, AggregateFunctionNullUnary<result_is_nullable, input_is_nullable>>
{
public:
    AggregateFunctionNullUnary(AggregateFunctionPtr nested_function)
        : AggregateFunctionNullBase<result_is_nullable, AggregateFunctionNullUnary<result_is_nullable, input_is_nullable>>(nested_function)
    {
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if constexpr (input_is_nullable)
        {
            const ColumnNullable * column = static_cast<const ColumnNullable *>(columns[0]);
            if (!column->isNullAt(row_num))
            {
                this->setFlag(place);
                const IColumn * nested_column = &column->getNestedColumn();
                this->nested_function->add(this->nestedPlace(place), &nested_column, row_num, arena);
            }
        }
        else
        {
            this->setFlag(place);
            this->nested_function->add(this->nestedPlace(place), columns, row_num, arena);
        }
    }

    void addBatchSinglePlace(
        size_t batch_size, AggregateDataPtr place, const IColumn ** columns, Arena * arena, ssize_t if_argument_pos = -1) const override
    {
        if (batch_size == 0)
            return;

        if constexpr (input_is_nullable)
        {
            const ColumnNullable * column = assert_cast<const ColumnNullable *>(columns[0]);
            const IColumn * nested_column = &column->getNestedColumn();
            const UInt8 * null_map = column->getNullMapData().data();

            this->nested_function->addBatchSinglePlaceNotNull(
                batch_size, this->nestedPlace(place), &nested_column, null_map, arena, if_argument_pos);

            if constexpr (result_is_nullable)
                if (!mem_utils::memoryIsByte(null_map, batch_size, std::byte{1}))
                    this->setFlag(place);
        }
        else
        {
            this->nested_function->addBatchSinglePlace(
                batch_size, this->nestedPlace(place), columns, arena, if_argument_pos);
            this->setFlag(place);
        }
    }
};


template <bool result_is_nullable>
class AggregateFunctionNullVariadic final : public AggregateFunctionNullBase<result_is_nullable, AggregateFunctionNullVariadic<result_is_nullable>>
{
public:
    AggregateFunctionNullVariadic(AggregateFunctionPtr nested_function, const DataTypes & arguments)
        : AggregateFunctionNullBase<result_is_nullable, AggregateFunctionNullVariadic<result_is_nullable>>(nested_function),
        number_of_arguments(arguments.size())
    {
        if (number_of_arguments == 1)
            throw Exception("Logical error: single argument is passed to AggregateFunctionNullVariadic", ErrorCodes::LOGICAL_ERROR);

        if (number_of_arguments > MAX_ARGS)
            throw Exception("Maximum number of arguments for aggregate function with Nullable types is " + toString(size_t(MAX_ARGS)),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (size_t i = 0; i < number_of_arguments; ++i)
            is_nullable[i] = arguments[i]->isNullable();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        /// This container stores the columns we really pass to the nested function.
        const IColumn * nested_columns[number_of_arguments];

        for (size_t i = 0; i < number_of_arguments; ++i)
        {
            if (is_nullable[i])
            {
                const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(*columns[i]);
                if (nullable_col.isNullAt(row_num))
                {
                    /// If at least one column has a null value in the current row,
                    /// we don't process this row.
                    return;
                }
                nested_columns[i] = &nullable_col.getNestedColumn();
            }
            else
                nested_columns[i] = columns[i];
        }

        this->setFlag(place);
        this->nested_function->add(this->nestedPlace(place), nested_columns, row_num, arena);
    }

    bool allocatesMemoryInArena() const override
    {
        return this->nested_function->allocatesMemoryInArena();
    }

private:
    enum { MAX_ARGS = 8 };
    size_t number_of_arguments = 0;
    std::array<char, MAX_ARGS> is_nullable;    /// Plain array is better than std::vector due to one indirection less.
};

/// a warp function on the top of groupArray and groupUniqArray

/// the input argument is in following two types:
/// 1. only one column with original data type and without order_by items
/// 2. one column combined with more than one columns including order by items, it should should be like tuple(type0, type1...)
template <bool result_is_nullable, bool only_one_argument>
class AggregateFunctionGroupConcat final : public AggregateFunctionNullBase<result_is_nullable, AggregateFunctionGroupConcat<result_is_nullable, only_one_argument>>
{
    using State = AggregateFunctionGroupUniqArrayGenericData;

public:
    AggregateFunctionGroupConcat(AggregateFunctionPtr nested_function, const DataTypes & input_args, const String sep, const UInt64& max_len_, const SortDescription & sort_desc_, const NamesAndTypes& all_arg_names_and_types_, const TiDB::TiDBCollators& collators_, const bool has_distinct)
    : AggregateFunctionNullBase<result_is_nullable, AggregateFunctionGroupConcat<result_is_nullable, only_one_argument>>(nested_function),
          separator(sep),max_len(max_len_), sort_desc(sort_desc_),
          all_arg_names_and_types(all_arg_names_and_types_), collators(collators_)
    {
        if (input_args.size() != 1)
            throw Exception("Logical error: not single argument is passed to AggregateFunctionGroupConcat", ErrorCodes::LOGICAL_ERROR);
        nested_type = std::make_shared<DataTypeArray>(removeNullable(input_args[0]));

        ///  all args =    concat args + order_by items
        /// (c0 + c1) = group_concat(c0 order by c1)
        number_of_concat_arguments = all_arg_names_and_types.size() - sort_desc.size();

        is_nullable.resize(number_of_concat_arguments);
        for (size_t i = 0; i < number_of_concat_arguments; ++i)
        {
            is_nullable[i] = all_arg_names_and_types[i].type->isNullable();
            /// the inputs of a nested agg reject null, but for more than one args, tuple(args...) is already not nullable,
            /// so here just remove null for the only_one_argument case
            if constexpr (only_one_argument)
            {
                all_arg_names_and_types[i].type = removeNullable(all_arg_names_and_types[i].type);
            }
        }

        /// remove redundant rows excluding extra sort items (which do not occur in the concat list) or considering collation
        if(has_distinct)
        {
            for (auto & desc : sort_desc)
            {
                bool is_extra = true;
                for (size_t i = 0; i < number_of_concat_arguments; ++i)
                {
                    if (desc.column_name == all_arg_names_and_types[i].name)
                    {
                        is_extra = false;
                        break;
                    }
                }
                if (is_extra)
                {
                    to_get_unique = true;
                    break;
                }
            }
            /// because GroupUniqArray does consider collations, so if there are collations,
            /// we should additionally remove redundant rows with consideration of collations
            if(!to_get_unique)
            {
                bool has_collation = false;
                for (size_t i = 0; i < number_of_concat_arguments; ++i)
                {
                    if (collators[i] != nullptr)
                    {
                        has_collation = true;
                        break;
                    }
                }
                to_get_unique = has_collation;
            }
        }
    }

    DataTypePtr getReturnType() const override
    {
        return result_is_nullable
               ? makeNullable(ret_type)
               : ret_type;
    }

    /// reject nulls before add() of nested agg
    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if constexpr (only_one_argument)
        {
            if(is_nullable[0])
            {
                const ColumnNullable * column = static_cast<const ColumnNullable *>(columns[0]);
                if (!column->isNullAt(row_num))
                {
                    this->setFlag(place);
                    const IColumn * nested_column = &column->getNestedColumn();
                    this->nested_function->add(this->nestedPlace(place), &nested_column, row_num, arena);
                }
                return;
            }
        }
        else
        {
            /// remove the row with null, except for sort columns
            const ColumnTuple & tuple = static_cast<const ColumnTuple &>(*columns[0]);
            for (size_t i = 0; i < number_of_concat_arguments; ++i)
            {
                if (is_nullable[i])
                {
                    const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(tuple.getColumn(i));
                    if (nullable_col.isNullAt(row_num))
                    {
                        /// If at least one column has a null value in the current row,
                        /// we don't process this row.
                        return;
                    }
                }
            }
        }
        this->setFlag(place);
        this->nested_function->add(this->nestedPlace(place), columns, row_num, arena);
    }

    void insertResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        ColumnString * col_str= nullptr;
        ColumnNullable * col_null= nullptr;
        if constexpr (result_is_nullable)
        {
            col_null = &static_cast<ColumnNullable &>(to);
            col_str = &static_cast<ColumnString &>(col_null->getNestedColumn());
        }
        else
        {
            col_str = & static_cast<ColumnString &>(to);
        }

        if (this->getFlag(place))
        {
            if constexpr (result_is_nullable)
            {
                col_null->getNullMapData().push_back(0);
            }

            /// get results from nested function, named nested_results
            auto mutable_nested_cols = nested_type->createColumn();
            this->nested_function->insertResultInto(this->nestedPlace(place), *mutable_nested_cols, arena);
            const auto column_array = checkAndGetColumn<ColumnArray>(mutable_nested_cols.get());

            /// nested_columns are not nullable, because the nullable rows are removed in add()
            Columns nested_cols;
            if constexpr (only_one_argument)
            {
                nested_cols.push_back(column_array->getDataPtr());
            }
            else
            {
                auto & cols =  checkAndGetColumn<ColumnTuple>(&column_array->getData())->getColumns();
                nested_cols.insert(nested_cols.begin(),cols.begin(),cols.end());
            }

            /// sort the nested_col of Array type
            if(!sort_desc.empty())
                sortColumns(nested_cols);

            /// get unique flags
            std::vector<bool> unique;
            if (to_get_unique)
                getUnique(nested_cols, unique);

            writeToStringColumn(nested_cols,col_str, unique);

        }
        else
        {
            if constexpr (result_is_nullable)
                col_null->insertDefault();
            else
                col_str->insertDefault();
        }
    }

    bool allocatesMemoryInArena() const override
    {
        return this->nested_function->allocatesMemoryInArena();
    }

private:
    /// construct a block to sort in the case with order-by requirement
    void sortColumns(Columns& nested_cols) const
    {
        Block res;
        int concat_size = nested_cols.size();
        for(int i = 0 ; i < concat_size; ++i )
        {
            res.insert(ColumnWithTypeAndName(nested_cols[i], all_arg_names_and_types[i].type, all_arg_names_and_types[i].name));
        }
        /// sort a block with collation
        sortBlock(res, sort_desc);
        nested_cols = res.getColumns();
    }

    /// get unique argument columns by inserting the unique of the first N of (N + M sort) internal columns within tuple
    void getUnique(const Columns & cols, std::vector<bool> & unique) const
    {
        std::unique_ptr<State> state = std::make_unique<State>();
        Arena arena1;
        auto size = cols[0]->size();
        unique.resize(size);
        std::vector<String> containers(collators.size());
        for (size_t i = 0; i < size; ++i)
        {
            bool inserted=false;
            State::Set::LookupResult it;
            const char * begin = nullptr;
            size_t values_size = 0;
            for (size_t j = 0; j< number_of_concat_arguments; ++j)
                values_size += cols[j]->serializeValueIntoArena(i, arena1, begin, collators[j],containers[j]).size;

            StringRef str_serialized= StringRef(begin, values_size);
            state->value.emplace(str_serialized, it, inserted);
            unique[i] = inserted;
        }
    }

    /// write each column cell to string with separator
    void writeToStringColumn(const Columns& cols, ColumnString * const col_str, const std::vector<bool> & unique) const
    {
        WriteBufferFromOwnString write_buffer;
        auto size = cols[0]->size();
        for (size_t i = 0; i < size; ++i)
        {
            if(unique.empty() || unique[i])
            {
                if (i != 0)
                {
                    writeString(separator, write_buffer);
                }
                for (size_t j = 0; j < number_of_concat_arguments; ++j)
                {
                    all_arg_names_and_types[j].type->serializeText(*cols[j], i, write_buffer);
                }
            }
            /// TODO(FZH) output just one warning ("Some rows were cut by GROUPCONCAT()") if this happen
            if(write_buffer.count() >=max_len)
            {
                break;
            }
        }
        col_str->insertData(write_buffer.str().c_str(),std::min(max_len,write_buffer.count()));
    }

    bool to_get_unique =false;
    DataTypePtr ret_type = std::make_shared<DataTypeString>();
    DataTypePtr nested_type;
    size_t number_of_concat_arguments = 0;
    String separator =",";
    UInt64 max_len;
    SortDescription sort_desc;
    NamesAndTypes all_arg_names_and_types;
    TiDB::TiDBCollators collators;
    std::vector<bool> is_nullable;    /// Plain array is better than std::vector due to one indirection less.
};

}
