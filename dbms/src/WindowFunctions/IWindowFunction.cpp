// Copyright 2022 PingCAP, Ltd.
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

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
} // namespace ErrorCodes

struct WindowFunctionRank final : public IWindowFunction
{
    static constexpr auto name = "rank";

    explicit WindowFunctionRank(const DataTypes & argument_types_)
        : IWindowFunction(argument_types_)
    {}

    String getName() const override
    {
        return name;
    }

    DataTypePtr getReturnType() const override
    {
        RUNTIME_CHECK(
            argument_types.size() == 0,
            Exception(
                fmt::format("Number of arguments for window function {} doesn't match: passed {}, should be 0.", getName(), argument_types.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH));
        return std::make_shared<DataTypeInt64>();
    }

    void windowInsertResultInto(
        WindowBlockInputStreamPtr stream,
        size_t function_index,
        [[maybe_unused]] const ColumnNumbers & arguments) override
    {
        assert(arguments.empty());
        IColumn & to = *stream->outputAt(stream->current_row)[function_index];
        assert_cast<ColumnInt64 &>(to).getData().push_back(
            stream->peer_group_start_row_number);
    }
};

struct WindowFunctionDenseRank final : public IWindowFunction
{
    static constexpr auto name = "dense_rank";

    explicit WindowFunctionDenseRank(const DataTypes & argument_types_)
        : IWindowFunction(argument_types_)
    {}

    String getName() const override
    {
        return name;
    }

    DataTypePtr getReturnType() const override
    {
        RUNTIME_CHECK(
            argument_types.size() == 0,
            Exception(
                fmt::format("Number of arguments for window function {} doesn't match: passed {}, should be 0.", getName(), argument_types.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH));
        return std::make_shared<DataTypeInt64>();
    }

    void windowInsertResultInto(
        WindowBlockInputStreamPtr stream,
        size_t function_index,
        [[maybe_unused]] const ColumnNumbers & arguments) override
    {
        assert(arguments.empty());
        IColumn & to = *stream->outputAt(stream->current_row)[function_index];
        assert_cast<ColumnInt64 &>(to).getData().push_back(
            stream->peer_group_number);
    }
};

struct WindowFunctionRowNumber final : public IWindowFunction
{
    static constexpr auto name = "row_number";

    explicit WindowFunctionRowNumber(const DataTypes & argument_types_)
        : IWindowFunction(argument_types_)
    {}

    String getName() const override
    {
        return name;
    }

    DataTypePtr getReturnType() const override
    {
        RUNTIME_CHECK(
            argument_types.size() == 0,
            Exception(
                fmt::format("Number of arguments for window function {} doesn't match: passed {}, should be 0.", getName(), argument_types.size()),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH));
        return std::make_shared<DataTypeInt64>();
    }

    void windowInsertResultInto(
        WindowBlockInputStreamPtr stream,
        size_t function_index,
        [[maybe_unused]] const ColumnNumbers & arguments) override
    {
        assert(arguments.empty());
        IColumn & to = *stream->outputAt(stream->current_row)[function_index];
        assert_cast<ColumnInt64 &>(to).getData().push_back(
            stream->current_row_number);
    }
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

    String getName() const override
    {
        return name;
    }

    DataTypePtr getReturnType() const override
    {
        return return_type;
    }

    // todo hanlde nullable, etc
    void windowInsertResultInto(
        WindowBlockInputStreamPtr stream,
        size_t function_index,
        const ColumnNumbers & arguments) override
    {
        const auto & cur_block = stream->blockAt(stream->current_row);

        IColumn & to = *cur_block.output_columns[function_index];

        auto offset = offset_getter(cur_block.input_columns, arguments, stream->current_row.row);
        auto value_row = stream->current_row;
        if (Impl::locateRowNumber(stream, value_row, offset))
        {
            const auto & value_column = *stream->inputAt(value_row)[arguments[0]];
            to.insertFrom(value_column, value_row.row);
        }
        else
        {
            default_value_setter(cur_block.input_columns, arguments, stream->current_row.row, to);
        }
    }

private:
    DataTypePtr getReturnTypeImpl() const
    {
        if (argument_types.size() >= 2)
        {
            auto second_argument = removeNullable(argument_types[1]);
            RUNTIME_CHECK(
                second_argument->isInteger(),
                Exception(
                    fmt::format("Illegal type {} of second argument of function {}", second_argument->getName(), getName()),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT));
        }
        if (argument_types.size() <= 2)
        {
            return removeNullable(argument_types[0])->getTypeId() == TypeIndex::Float32
                ? makeNullable(std::make_shared<DataTypeFloat64>())
                : makeNullable(argument_types[0]);
        }
        else
        {
            return FunctionIf{}.getReturnTypeImpl({std::make_shared<DataTypeUInt8>(), argument_types[0], argument_types[2]});
        }
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
                to.insertFrom(default_value_column, row);
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
                throw Exception(fmt::format("the argument type of {} is invalid, expect integer, got {}", getName(), type_index), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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

    static bool locateRowNumber(
        const WindowBlockInputStreamPtr & stream,
        RowNumber & value_row,
        UInt64 offset)
    {
        return stream->advanceRowNumber(value_row, offset);
    }
};

struct LagImpl
{
    static constexpr auto name = "lag";

    static bool locateRowNumber(
        const WindowBlockInputStreamPtr & stream,
        RowNumber & value_row,
        UInt64 offset)
    {
        return stream->backRowNumber(value_row, offset);
    }
};

void registerWindowFunctions(WindowFunctionFactory & factory)
{
    factory.registerFunction<WindowFunctionRank>();
    factory.registerFunction<WindowFunctionDenseRank>();
    factory.registerFunction<WindowFunctionRowNumber>();
    factory.registerFunction<WindowFunctionLeadLagBase<LeadImpl>>();
    factory.registerFunction<WindowFunctionLeadLagBase<LagImpl>>();
}
} // namespace DB
