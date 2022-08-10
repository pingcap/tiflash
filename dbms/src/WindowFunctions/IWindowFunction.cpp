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

#include <DataStreams/WindowBlockInputStream.h>
#include <DataTypes/getLeastSupertype.h>
#include <WindowFunctions/IWindowFunction.h>
#include <WindowFunctions/WindowFunctionFactory.h>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
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
        return std::make_shared<DataTypeInt64>();
    }

    void windowInsertResultInto(WindowBlockInputStreamPtr stream,
                                size_t function_index) override
    {
        IColumn & to = *stream->blockAt(stream->current_row)
                            .output_columns[function_index];
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
        return std::make_shared<DataTypeInt64>();
    }

    void windowInsertResultInto(WindowBlockInputStreamPtr stream,
                                size_t function_index) override
    {
        IColumn & to = *stream->blockAt(stream->current_row)
                            .output_columns[function_index];
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
        return std::make_shared<DataTypeInt64>();
    }

    void windowInsertResultInto(WindowBlockInputStreamPtr stream,
                                size_t function_index) override
    {
        IColumn & to = *stream->blockAt(stream->current_row)
                            .output_columns[function_index];
        assert_cast<ColumnInt64 &>(to).getData().push_back(
            stream->current_row_number);
    }
};

void registerWindowFunctions(WindowFunctionFactory & factory)
{
    factory.registerFunction<WindowFunctionRank>();
    factory.registerFunction<WindowFunctionDenseRank>();
    factory.registerFunction<WindowFunctionRowNumber>();
}
} // namespace DB
