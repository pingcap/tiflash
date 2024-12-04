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

#pragma once

#include <AggregateFunctions/AggregateFunctionGroupUniqArray.h>
#include <AggregateFunctions/AggregateFunctionNull.h>


namespace DB
{
/// a warp function on the top of groupArray and groupUniqArray, like the AggregateFunctionNull
///
/// the input argument is in following two types:
/// 1. only one column with original data type and without order_by items, for example: group_concat(c)
/// 2. one column combined with more than one columns including concat items and order-by items, it should be like tuple(concat0, concat1... order0, order1 ...), for example:
///  all columns  =      concat items + order-by items
/// (c0,c1,o0,o1) = group_concat(c0,c1 order by o0,o1)
/// group_concat(distinct c0,c1 order by b0,b1) = groupUniqArray(tuple(c0,c1,b0,b1)) -> distinct (c0, c1) , i.e., remove duplicates further

template <bool result_is_nullable, bool only_one_column>
class AggregateFunctionGroupConcat final
    : public AggregateFunctionNullBase<
          result_is_nullable,
          AggregateFunctionGroupConcat<result_is_nullable, only_one_column>>
{
    using State = AggregateFunctionGroupUniqArrayGenericData;

public:
    AggregateFunctionGroupConcat(
        AggregateFunctionPtr nested_function,
        const DataTypes & input_args,
        const String & sep,
        const UInt64 & max_len_,
        const SortDescription & sort_desc_,
        const NamesAndTypes & all_columns_names_and_types_,
        const TiDB::TiDBCollators & collators_,
        const bool has_distinct)
        : AggregateFunctionNullBase<
            result_is_nullable,
            AggregateFunctionGroupConcat<result_is_nullable, only_one_column>>(nested_function)
        , separator(sep)
        , max_len(max_len_)
        , sort_desc(sort_desc_)
        , all_columns_names_and_types(all_columns_names_and_types_)
        , collators(collators_)
    {
        if (input_args.size() != 1)
            throw Exception(
                "Logical error: more than 1 arguments are passed to AggregateFunctionGroupConcat",
                ErrorCodes::LOGICAL_ERROR);
        nested_type = std::make_shared<DataTypeArray>(removeNullable(input_args[0]));

        number_of_concat_items = all_columns_names_and_types.size() - sort_desc.size();

        is_nullable.resize(number_of_concat_items);
        for (size_t i = 0; i < number_of_concat_items; ++i)
        {
            is_nullable[i] = all_columns_names_and_types[i].type->isNullable();
            /// the inputs of a nested agg reject null, but for more than one args, tuple(args...) is already not nullable,
            /// so here just remove null for the only_one_column case
            if constexpr (only_one_column)
            {
                all_columns_names_and_types[i].type = removeNullable(all_columns_names_and_types[i].type);
            }
        }

        /// remove redundant rows excluding extra sort items (which do not occur in the concat list) or considering collation
        if (has_distinct)
        {
            for (auto & desc : sort_desc)
            {
                bool is_extra = true;
                for (size_t i = 0; i < number_of_concat_items; ++i)
                {
                    if (desc.column_name == all_columns_names_and_types[i].name)
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
            if (!to_get_unique)
            {
                bool has_collation = false;
                for (size_t i = 0; i < number_of_concat_items; ++i)
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

    DataTypePtr getReturnType() const override { return result_is_nullable ? makeNullable(ret_type) : ret_type; }

    /// reject nulls before add()/decrease() of nested agg
    template<bool is_add>
    void addOrDecrease(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const
    {
        if constexpr (only_one_column)
        {
            if (is_nullable[0])
            {
                const auto * column = static_cast<const ColumnNullable *>(columns[0]);
                if (!column->isNullAt(row_num))
                {
                    this->setFlag(place);
                    const IColumn * nested_column = &column->getNestedColumn();

                    if constexpr (is_add)
                        this->nested_function->add(this->nestedPlace(place), &nested_column, row_num, arena);
                    else
                        this->nested_function->decrease(this->nestedPlace(place), &nested_column, row_num, arena);
                }
                return;
            }
        }
        else
        {
            /// remove the row with null, except for sort columns
            const auto & tuple = static_cast<const ColumnTuple &>(*columns[0]);
            for (size_t i = 0; i < number_of_concat_items; ++i)
            {
                if (is_nullable[i])
                {
                    const auto & nullable_col = static_cast<const ColumnNullable &>(tuple.getColumn(i));
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
        if constexpr (is_add)
            this->nested_function->add(this->nestedPlace(place), columns, row_num, arena);
        else
            this->nested_function->decrease(this->nestedPlace(place), columns, row_num, arena);
    }
    
    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        addOrDecrease<true>(place, columns, row_num, arena);
    }

    void decrease(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        addOrDecrease<false>(place, columns, row_num, arena);
    }

    void insertResultInto(ConstAggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        ColumnString * col_str = nullptr;
        ColumnNullable * col_null = nullptr;
        if constexpr (result_is_nullable)
        {
            col_null = &static_cast<ColumnNullable &>(to);
            col_str = &static_cast<ColumnString &>(col_null->getNestedColumn());
        }
        else
        {
            col_str = &static_cast<ColumnString &>(to);
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
            if constexpr (only_one_column)
            {
                nested_cols.push_back(column_array->getDataPtr());
            }
            else
            {
                auto & cols = checkAndGetColumn<ColumnTuple>(&column_array->getData())->getColumns();
                nested_cols.insert(nested_cols.begin(), cols.begin(), cols.end());
            }

            /// sort the nested_col of Array type
            if (!sort_desc.empty())
                sortColumns(nested_cols);

            /// get unique flags
            std::vector<bool> unique;
            if (to_get_unique)
                getUnique(nested_cols, unique);

            writeToStringColumn(nested_cols, col_str, unique);
        }
        else
        {
            if constexpr (result_is_nullable)
                col_null->insertDefault();
            else
                col_str->insertDefault();
        }
    }

    bool allocatesMemoryInArena() const override { return this->nested_function->allocatesMemoryInArena(); }

private:
    /// construct a block to sort in the case with order-by requirement
    void sortColumns(Columns & nested_cols) const
    {
        Block res;
        int concat_size = nested_cols.size();
        for (int i = 0; i < concat_size; ++i)
        {
            res.insert(ColumnWithTypeAndName(
                nested_cols[i],
                all_columns_names_and_types[i].type,
                all_columns_names_and_types[i].name));
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
            bool inserted = false;
            State::Set::LookupResult it;
            const char * begin = nullptr;
            size_t values_size = 0;
            for (size_t j = 0; j < number_of_concat_items; ++j)
                values_size += cols[j]->serializeValueIntoArena(i, arena1, begin, collators[j], containers[j]).size;

            StringRef str_serialized = StringRef(begin, values_size);
            state->value.emplace(str_serialized, it, inserted);
            unique[i] = inserted;
        }
    }

    /// write each column cell to string with separator
    void writeToStringColumn(const Columns & cols, ColumnString * const col_str, const std::vector<bool> & unique) const
    {
        WriteBufferFromOwnString write_buffer;
        auto size = cols[0]->size();
        for (size_t i = 0; i < size; ++i)
        {
            if (unique.empty() || unique[i])
            {
                if (i != 0)
                {
                    writeString(separator, write_buffer);
                }
                for (size_t j = 0; j < number_of_concat_items; ++j)
                {
                    all_columns_names_and_types[j].type->serializeText(*cols[j], i, write_buffer);
                }
            }
            /// TODO(FZH) output just one warning ("Some rows were cut by GROUPCONCAT()") if this happen
            if (write_buffer.count() >= max_len)
            {
                break;
            }
        }
        col_str->insertData(write_buffer.str().c_str(), std::min(max_len, write_buffer.count()));
    }

    bool to_get_unique = false;
    DataTypePtr ret_type = std::make_shared<DataTypeString>();
    DataTypePtr nested_type;
    size_t number_of_concat_items = 0;
    String separator = ",";
    UInt64 max_len;
    SortDescription sort_desc;
    NamesAndTypes all_columns_names_and_types;
    TiDB::TiDBCollators collators;
    BoolVec is_nullable;
};
} // namespace DB
