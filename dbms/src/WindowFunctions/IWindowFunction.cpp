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
    WindowFunctionRank(const std::string & name_,
                       const DataTypes & argument_types_)
        : IWindowFunction(name_, argument_types_)
    {}

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
    WindowFunctionDenseRank(const std::string & name_,
                            const DataTypes & argument_types_)
        : IWindowFunction(name_, argument_types_)
    {}

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
    WindowFunctionRowNumber(const std::string & name_,
                            const DataTypes & argument_types_)
        : IWindowFunction(name_, argument_types_)
    {}

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
    factory.registerFunction(
        "rank",
        [](const std::string & name, const DataTypes & argument_types) { return std::make_shared<WindowFunctionRank>(name, argument_types); });
    factory.registerFunction(
        "dense_rank",
        [](const std::string & name, const DataTypes & argument_types) { return std::make_shared<WindowFunctionDenseRank>(name, argument_types); });
    factory.registerFunction(
        "row_number",
        [](const std::string & name, const DataTypes & argument_types) { return std::make_shared<WindowFunctionRowNumber>(name, argument_types); });
}
} // namespace DB
