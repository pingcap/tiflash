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

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <DataStreams/WindowBlockInputStream.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionsConditional.h>
#include <WindowFunctions/IWindowFunction.h>
#include <WindowFunctions/WindowFunctionFactory.h>

#include <magic_enum.hpp>
#include <memory>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
} // namespace ErrorCodes

namespace
{
bool isFrameValid(RowNumber frame_start, RowNumber frame_end, RowNumber partition_start, RowNumber partition_end)
{
    return (frame_start < partition_end) && (partition_start < frame_end) && (frame_start < frame_end);
}

// Return true when the frame is valid
//
// Check if the frame is invalid, or we will insert null value into result column
// Frame is invalid when:
//   1. partition_end <= frame_start
//   2. frame_end < partition_start
//   3. frame_end <= frame_start
bool checkFrameValidAndHandleInvalidFrame(WindowTransformAction & action, size_t function_index)
{
    if (isFrameValid(action.frame_start, action.frame_end, action.partition_start, action.partition_end))
        return true;

    IColumn & to = *action.blockAt(action.current_row).output_columns[function_index];
    static Field null_field;
    to.insert(null_field);
    return false;
}
} // namespace

// In window function, some of the functions must return nullable column.
// Return nullable column functions: FIRST_VALUE, LAST_VALUE, NTH_VALUE, NTILE and all aggregation functions,
//                                   LEAD/LAG(when receiving less than 3 parameters or when receiving 3 parameters
//                                            one of the first and the third parameters is related with nullable)
// Return not nullable column functions: CUME_DIST, DENSE_RANK, PERCENT_RANK, ROW_NUMBER, RANK,
//                                       LEAD/LAG(When receiving 3 parameters and the first and the third parameters
//                                                are both not related with nullable)

struct WindowFunctionRank final : public IWindowFunction
{
    static constexpr auto name = "rank";

    explicit WindowFunctionRank(const DataTypes & argument_types_)
        : IWindowFunction(argument_types_)
    {}

    String getName() const override { return name; }

    DataTypePtr getReturnType() const override
    {
        RUNTIME_CHECK_MSG(
            argument_types.empty(),
            "Number of arguments for window function {} doesn't match: passed {}, should be 0.",
            getName(),
            argument_types.size());
        return std::make_shared<DataTypeInt64>();
    }

    void windowInsertResultInto(WindowTransformAction & action, size_t function_index, const ColumnNumbers &) override
    {
        IColumn & to = *action.outputAt(action.current_row)[function_index];
        assert_cast<ColumnInt64 &>(to).getData().push_back(action.peer_group_start_row_number);
    }
};

struct WindowFunctionDenseRank final : public IWindowFunction
{
    static constexpr auto name = "dense_rank";

    explicit WindowFunctionDenseRank(const DataTypes & argument_types_)
        : IWindowFunction(argument_types_)
    {}

    String getName() const override { return name; }

    DataTypePtr getReturnType() const override
    {
        RUNTIME_CHECK_MSG(
            argument_types.empty(),
            "Number of arguments for window function {} doesn't match: passed {}, should be 0.",
            getName(),
            argument_types.size());
        return std::make_shared<DataTypeInt64>();
    }

    void windowInsertResultInto(WindowTransformAction & action, size_t function_index, const ColumnNumbers &) override
    {
        IColumn & to = *action.outputAt(action.current_row)[function_index];
        assert_cast<ColumnInt64 &>(to).getData().push_back(action.peer_group_number);
    }
};

struct WindowFunctionRowNumber final : public IWindowFunction
{
    static constexpr auto name = "row_number";

    explicit WindowFunctionRowNumber(const DataTypes & argument_types_)
        : IWindowFunction(argument_types_)
    {}

    String getName() const override { return name; }

    DataTypePtr getReturnType() const override
    {
        RUNTIME_CHECK_MSG(
            argument_types.empty(),
            "Number of arguments for window function {} doesn't match: passed {}, should be 0.",
            getName(),
            argument_types.size());
        return std::make_shared<DataTypeInt64>();
    }

    void windowInsertResultInto(WindowTransformAction & action, size_t function_index, const ColumnNumbers &) override
    {
        IColumn & to = *action.outputAt(action.current_row)[function_index];
        assert_cast<ColumnInt64 &>(to).getData().push_back(action.current_row_number);
    }
};

struct WindowFunctionFirstValue final : public IWindowFunction
{
public:
    static constexpr auto name = "first_value";

    explicit WindowFunctionFirstValue(const DataTypes & argument_types_)
        : IWindowFunction(argument_types_)
    {
        RUNTIME_CHECK(argument_types_.size() == 1);
        return_type = makeNullable(argument_types_[0]);
    }

    String getName() const override { return name; }

    DataTypePtr getReturnType() const override { return return_type; }

    void windowInsertResultInto(WindowTransformAction & action, size_t function_index, const ColumnNumbers & arguments)
        override
    {
        assert(action.frame_started);
        if (!checkFrameValidAndHandleInvalidFrame(action, function_index))
            return;

        IColumn & to = *action.blockAt(action.current_row).output_columns[function_index];
        const auto & value_column = *action.inputAt(action.frame_start)[arguments[0]];
        const auto & value_field = value_column[action.frame_start.row];
        to.insert(value_field);
    }

private:
    DataTypePtr return_type;
};

struct WindowFunctionLastValue final : public IWindowFunction
{
public:
    static constexpr auto name = "last_value";

    explicit WindowFunctionLastValue(const DataTypes & argument_types_)
        : IWindowFunction(argument_types_)
    {
        RUNTIME_CHECK(argument_types_.size() == 1);
        return_type = makeNullable(argument_types_[0]);
    }

    String getName() const override { return name; }

    DataTypePtr getReturnType() const override { return return_type; }

    void windowInsertResultInto(WindowTransformAction & action, size_t function_index, const ColumnNumbers & arguments)
        override
    {
        assert(action.frame_ended);
        if (!checkFrameValidAndHandleInvalidFrame(action, function_index))
            return;

        IColumn & to = *action.blockAt(action.current_row).output_columns[function_index];

        // Because [frame_start, frame_end), so we need to get the previous row of the frame_end.
        RowNumber last_row = action.getPreviousRowNumber(action.frame_end);

        const auto & value_column = *action.inputAt(last_row)[arguments[0]];
        const auto & value_field = value_column[last_row.row];
        to.insert(value_field);
    }

private:
    DataTypePtr return_type;
};

/**
LEAD/LAG(<expression>[,offset[, default_value]]) OVER (
    PARTITION BY (expr)
    ORDER BY (expr)
)
 * */
template <typename Impl>
class WindowFunctionLeadLagBase : public IWindowFunction
{
public:
    static constexpr auto name = Impl::name;

    explicit WindowFunctionLeadLagBase(const DataTypes & argument_types_)
        : IWindowFunction(argument_types_)
    {
        return_type = getReturnTypeImpl();
        offset_getter = initOffsetGetter();
        default_value_setter = initDefaultValueSetter();
    }

    String getName() const override { return name; }

    DataTypePtr getReturnType() const override { return return_type; }

    void windowInsertResultInto(WindowTransformAction & action, size_t function_index, const ColumnNumbers & arguments)
        override
    {
        const auto & cur_block = action.blockAt(action.current_row);

        IColumn & to = *cur_block.output_columns[function_index];

        auto offset = offset_getter(cur_block.input_columns, arguments, action.current_row.row);
        auto value_row = action.current_row;
        if (Impl::locate(action, value_row, offset))
        {
            const auto & value_column = *action.inputAt(value_row)[arguments[0]];
            const auto & value_field = value_column[value_row.row];
            to.insert(value_field);
        }
        else
        {
            default_value_setter(cur_block.input_columns, arguments, action.current_row.row, to);
        }
    }

private:
    DataTypePtr getReturnTypeImpl() const
    {
        size_t argument_num = argument_types.size();
        RUNTIME_CHECK_MSG(
            1 <= argument_num && argument_num <= 3,
            "argument num {} of function {} isn't in [1, 3]",
            argument_num,
            name);
        if (argument_num >= 2)
        {
            auto second_argument = removeNullable(argument_types[1]);
            RUNTIME_CHECK_MSG(
                second_argument->isInteger(),
                "Illegal type {} of second argument of function {}",
                second_argument->getName(),
                name);
        }
        if (argument_num == 3)
        {
            auto first_argument = removeNullable(argument_types[0]);
            auto third_argument = removeNullable(argument_types[2]);
            RUNTIME_CHECK_MSG(
                third_argument->equals(*first_argument),
                "type {} of first argument is different from type {} of third argument of function {}",
                first_argument->getName(),
                third_argument->getName(),
                name);
        }
        return argument_num < 3 ? makeNullable(argument_types[0]) : argument_types[0];
    }

    using DefaultValueSetter = std::function<void(const Columns &, const ColumnNumbers &, size_t, IColumn &)>;

    DefaultValueSetter initDefaultValueSetter()
    {
        if (argument_types.size() < 3)
        {
            return [](const Columns &, const ColumnNumbers &, size_t, IColumn & to) {
                static Field null_field;
                to.insert(null_field);
            };
        }
        else
        {
            return [](const Columns & input_columns, const ColumnNumbers & arguments, size_t row, IColumn & to) {
                const auto & default_value_column = *input_columns[arguments[2]];
                to.insert(default_value_column[row]);
            };
        }
    }

    using OffsetGetter = std::function<UInt64(const Columns &, const ColumnNumbers &, size_t)>;

    OffsetGetter initOffsetGetter()
    {
        if (argument_types.size() < 2)
        {
            return [](const Columns &, const ColumnNumbers &, size_t) -> UInt64 {
                return 1;
            };
        }
        else
        {
            auto type_index = argument_types[1]->getTypeId();
            switch (type_index)
            {
#define M(T)                                                                                        \
    case TypeIndex::T:                                                                              \
        return [](const Columns & columns, const ColumnNumbers & arguments, size_t row) -> UInt64 { \
            const IColumn & offset_column = *columns[arguments[1]];                                 \
            T origin_value = offset_column[row].get<T>();                                           \
            if constexpr (std::is_signed_v<T>)                                                      \
            {                                                                                       \
                if (origin_value < 0)                                                               \
                    return 0;                                                                       \
            }                                                                                       \
            return static_cast<size_t>(origin_value);                                               \
        };
                M(UInt8)
                M(UInt16)
                M(UInt32)
                M(UInt64)
                M(Int8)
                M(Int16)
                M(Int32)
                M(Int64)
#undef M
            default:
                throw Exception(
                    fmt::format(
                        "the argument type of {} is invalid, expect integer, got {}",
                        name,
                        magic_enum::enum_name(type_index)),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            };
        }
    }

private:
    DataTypePtr return_type;
    OffsetGetter offset_getter;
    DefaultValueSetter default_value_setter;
};

struct LeadImpl
{
    static constexpr auto name = "lead";

    static bool locate(const WindowTransformAction & action, RowNumber & value_row, UInt64 offset)
    {
        return action.lead(value_row, offset);
    }
};

struct LagImpl
{
    static constexpr auto name = "lag";

    static bool locate(const WindowTransformAction & action, RowNumber & value_row, UInt64 offset)
    {
        return action.lag(value_row, offset);
    }
};

void registerWindowFunctions(WindowFunctionFactory & factory)
{
    factory.registerFunction<WindowFunctionRank>();
    factory.registerFunction<WindowFunctionDenseRank>();
    factory.registerFunction<WindowFunctionRowNumber>();
    factory.registerFunction<WindowFunctionLeadLagBase<LeadImpl>>();
    factory.registerFunction<WindowFunctionLeadLagBase<LagImpl>>();
    factory.registerFunction<WindowFunctionFirstValue>();
    factory.registerFunction<WindowFunctionLastValue>();
}
} // namespace DB
