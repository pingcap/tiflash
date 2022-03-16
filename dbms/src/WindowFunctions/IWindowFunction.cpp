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
        return std::make_shared<DataTypeUInt64>();
    }

    void windowInsertResultInto(WindowBlockInputStreamPtr stream,
                                size_t function_index) override
    {
        IColumn & to = *stream->blockAt(stream->current_row)
                            .output_columns[function_index];
        assert_cast<ColumnUInt64 &>(to).getData().push_back(
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
        return std::make_shared<DataTypeUInt64>();
    }


    void windowInsertResultInto(WindowBlockInputStreamPtr stream,
                                size_t function_index) override
    {
        IColumn & to = *stream->blockAt(stream->current_row)
                            .output_columns[function_index];
        assert_cast<ColumnUInt64 &>(to).getData().push_back(
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
        return std::make_shared<DataTypeUInt64>();
    }


    void windowInsertResultInto(WindowBlockInputStreamPtr stream,
                                size_t function_index) override
    {
        IColumn & to = *stream->blockAt(stream->current_row)
                            .output_columns[function_index];
        assert_cast<ColumnUInt64 &>(to).getData().push_back(
            stream->current_row_number);
    }
};

void registerWindowFunctions(WindowFunctionFactory & factory)
{
    factory.registerFunction(
        "rank",
        [](const std::string & name, const DataTypes & argument_types) { return std::make_shared<WindowFunctionRank>(name, argument_types); },
        WindowFunctionFactory::CaseInsensitive);
    factory.registerFunction(
        "dense_rank",
        [](const std::string & name, const DataTypes & argument_types) { return std::make_shared<WindowFunctionDenseRank>(name, argument_types); },
        WindowFunctionFactory::CaseInsensitive);
    factory.registerFunction(
        "row_number",
        [](const std::string & name, const DataTypes & argument_types) { return std::make_shared<WindowFunctionRowNumber>(name, argument_types); },
        WindowFunctionFactory::CaseInsensitive);
}
} // namespace DB
