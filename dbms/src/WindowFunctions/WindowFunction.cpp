#include <DataStreams/WindowBlockInputStream.h>
#include <DataTypes/getLeastSupertype.h>
#include <WindowFunctions/WindowFunction.h>
#include <WindowFunctions/WindowFunctionFactory.h>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

struct WindowFunctionRank final : public WindowFunction
{
    WindowFunctionRank(const std::string & name_,
                       const DataTypes & argument_types_)
        : WindowFunction(name_, argument_types_)
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

struct WindowFunctionDenseRank final : public WindowFunction
{
    WindowFunctionDenseRank(const std::string & name_,
                            const DataTypes & argument_types_)
        : WindowFunction(name_, argument_types_)
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

struct WindowFunctionRowNumber final : public WindowFunction
{
    WindowFunctionRowNumber(const std::string & name_,
                            const DataTypes & argument_types_)
        : WindowFunction(name_, argument_types_)
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

// ClickHouse-specific variant of lag/lead that respects the window frame.
template <bool is_lead>
struct WindowFunctionLagLeadInFrame final : public WindowFunction
{
    WindowFunctionLagLeadInFrame(const std::string & name_,
                                 const DataTypes & argument_types_)
        : WindowFunction(name_, argument_types_)
    {
        if (argument_types.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Function {} takes at least one argument",
                            name_);
        }

        if (argument_types.size() == 1)
        {
            return;
        }

        if (!isInt64OrUInt64FieldType(argument_types[1]->getDefault().getType()))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Offset must be an integer, '{}' given",
                            argument_types[1]->getName());
        }

        if (argument_types.size() == 2)
        {
            return;
        }

        const auto supertype = getLeastSupertype({argument_types[0], argument_types[2]});
        if (!supertype)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "There is no supertype for the argument type '{}' and the default value type '{}'",
                            argument_types[0]->getName(),
                            argument_types[2]->getName());
        }
        if (!argument_types[0]->equals(*supertype))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "The supertype '{}' for the argument type '{}' and the default value type '{}' is not the same as the argument type",
                            supertype->getName(),
                            argument_types[0]->getName(),
                            argument_types[2]->getName());
        }

        if (argument_types.size() > 3)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Function '{}' accepts at most 3 arguments, {} given",
                            name,
                            argument_types.size());
        }
    }

    DataTypePtr getReturnType() const override { return argument_types[0]; }

    void windowInsertResultInto(WindowBlockInputStreamPtr stream,
                                size_t function_index) override
    {
        const auto & current_block = stream->blockAt(stream->current_row);
        IColumn & to = *stream->blockAt(stream->current_row).output_columns[function_index];
        const auto & workspace = stream->workspaces[function_index];

        int64_t offset = 1;
        if (argument_types.size() > 1)
        {
            offset = (*current_block.input_columns[workspace.argument_column_indices[1]])[stream->current_row.row].get<Int64>();

            /// Either overflow or really negative value, both is not acceptable.
            if (offset < 0)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "The offset for function {} must be in (0, {}], {} given",
                                name,
                                INT64_MAX,
                                offset);
            }
        }

        const auto [target_row, offset_left] = stream->moveRowNumber(
            stream->current_row,
            offset * (is_lead ? 1 : -1));

        if (offset_left != 0
            || target_row < stream->frame_start
            || stream->frame_end <= target_row)
        {
            // Offset is outside the frame.
            if (argument_types.size() > 2)
            {
                // Column with default values is specified.
                // The conversion through Field is inefficient, but we accept
                // subtypes of the argument type as a default value (for convenience),
                // and it's a pain to write conversion that respects ColumnNothing
                // and ColumnConst and so on.
                const IColumn & default_column = *current_block.input_columns[workspace.argument_column_indices[2]].get();
                to.insert(default_column[stream->current_row.row]);
            }
            else
            {
                to.insertDefault();
            }
        }
        else
        {
            // Offset is inside the frame.
            to.insertFrom(*stream->blockAt(target_row).input_columns[workspace.argument_column_indices[0]],
                          target_row.row);
        }
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
    factory.registerFunction(
        "lag_in_frame",
        [](const std::string & name, const DataTypes & argument_types) { return std::make_shared<WindowFunctionLagLeadInFrame<false>>(name, argument_types); },
        WindowFunctionFactory::CaseInsensitive);
    factory.registerFunction(
        "lead_in_frame",
        [](const std::string & name, const DataTypes & argument_types) { return std::make_shared<WindowFunctionLagLeadInFrame<true>>(name, argument_types); },
        WindowFunctionFactory::CaseInsensitive);
}

} // namespace DB
