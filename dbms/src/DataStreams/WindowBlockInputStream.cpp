#include <DataStreams/WindowBlockInputStream.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Common/Arena.h>
#include <Common/FieldVisitors.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/convertFieldToType.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
} // namespace ErrorCodes

class IWindowFunction
{
public:
    virtual ~IWindowFunction() = default;

    // Must insert the result for current_row.
    virtual void windowInsertResultInto(
                                        size_t function_index) = 0;
};

// A basic implementation for a true window function. It pretends to be an
// aggregate function, but refuses to work as such.
struct WindowFunction
    : public IAggregateFunctionHelper<WindowFunction>
        , public IWindowFunction
{
    std::string name;

    WindowFunction(const std::string & name_, const DataTypes & argument_types_, const Array & parameters_)
        : name(name_)
    {}

    bool isOnlyWindowFunction() const override { return true; }

    [[noreturn]] void fail() const
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "The function '{}' can only be used as a window function, not as an aggregate function",
                        getName());
    }

    String getName() const override { return name; }
    void create(AggregateDataPtr __restrict) const override { fail(); }
    void destroy(AggregateDataPtr __restrict) const noexcept override {}
    bool hasTrivialDestructor() const override { return true; }
    size_t sizeOfData() const override { return 0; }
    size_t alignOfData() const override { return 1; }
    void add(AggregateDataPtr __restrict, const IColumn **, size_t, Arena *) const override { fail(); }
    void merge(AggregateDataPtr __restrict, ConstAggregateDataPtr, Arena *) const override { fail(); }
    void serialize(ConstAggregateDataPtr __restrict, WriteBuffer &) const override { fail(); }
    void deserialize(AggregateDataPtr __restrict, ReadBuffer &, Arena *) const override { fail(); }
    void insertResultInto(ConstAggregateDataPtr __restrict, IColumn &, Arena *) const override { fail(); }
    const char * getHeaderFilePath() const override { fail(); }
};

struct WindowFunctionRank final : public WindowFunction
{
    WindowFunctionRank(const std::string & name_,
                       const DataTypes & argument_types_, const Array & parameters_)
        : WindowFunction(name_, argument_types_, parameters_)
    {}

    DataTypePtr getReturnType() const override
    { return std::make_shared<DataTypeUInt64>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(size_t function_index) override
    {
//        IColumn & to = *transform->blockAt(transform->current_row)
//            .output_columns[function_index];
//        assert_cast<ColumnUInt64 &>(to).getData().push_back(
//            transform->peer_group_start_row_number);
    }
};

struct WindowFunctionDenseRank final : public WindowFunction
{
    WindowFunctionDenseRank(const std::string & name_,
                            const DataTypes & argument_types_, const Array & parameters_)
        : WindowFunction(name_, argument_types_, parameters_)
    {}

    DataTypePtr getReturnType() const override
    { return std::make_shared<DataTypeUInt64>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(
                                size_t function_index) override
    {
  /*      IColumn & to = *transform->blockAt(transform->current_row)
            .output_columns[function_index];
        assert_cast<ColumnUInt64 &>(to).getData().push_back(
            transform->peer_group_number);*/
    }
};

struct WindowFunctionRowNumber final : public WindowFunction
{
    WindowFunctionRowNumber(const std::string & name_,
                            const DataTypes & argument_types_, const Array & parameters_)
        : WindowFunction(name_, argument_types_, parameters_)
    {}

    DataTypePtr getReturnType() const override
    { return std::make_shared<DataTypeUInt64>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(
        size_t function_index) override
    {
        /*      IColumn & to = *transform->blockAt(transform->current_row)
                  .output_columns[function_index];
              assert_cast<ColumnUInt64 &>(to).getData().push_back(
                  transform->peer_group_number);*/
    }
};

// ClickHouse-specific variant of lag/lead that respects the window frame.
template <bool is_lead>
struct WindowFunctionLagLeadInFrame final : public WindowFunction
{
    DataTypes argument_types;
    Array parameters;

    WindowFunctionLagLeadInFrame(const std::string & name_,
                                 const DataTypes & argument_types_, const Array & parameters_)
        : argument_types(argument_types_), parameters(parameters_),  WindowFunction(name_, argument_types_, parameters_)
    {
        if (!parameters.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Function {} cannot be parameterized", name_);
        }

        if (argument_types.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Function {} takes at least one argument", name_);
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
                            name, argument_types.size());
        }
    }

    DataTypePtr getReturnType() const override { return argument_types[0]; }

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(
                                size_t function_index) override
    {
//        const auto & current_block = transform->blockAt(transform->current_row);
//        IColumn & to = *current_block.output_columns[function_index];
//        const auto & workspace = transform->workspaces[function_index];
//
//        int64_t offset = 1;
//        if (argument_types.size() > 1)
//        {
//            offset = (*current_block.input_columns[
//                workspace.argument_column_indices[1]])[
//                transform->current_row.row].get<Int64>();
//
//            /// Either overflow or really negative value, both is not acceptable.
//            if (offset < 0)
//            {
//                throw Exception(ErrorCodes::BAD_ARGUMENTS,
//                                "The offset for function {} must be in (0, {}], {} given",
//                                getName(), INT64_MAX, offset);
//            }
//        }
//
//        const auto [target_row, offset_left] = transform->moveRowNumber(
//            transform->current_row, offset * (is_lead ? 1 : -1));
//
//        if (offset_left != 0
//            || target_row < transform->frame_start
//            || transform->frame_end <= target_row)
//        {
//            // Offset is outside the frame.
//            if (argument_types.size() > 2)
//            {
//                // Column with default values is specified.
//                // The conversion through Field is inefficient, but we accept
//                // subtypes of the argument type as a default value (for convenience),
//                // and it's a pain to write conversion that respects ColumnNothing
//                // and ColumnConst and so on.
//                const IColumn & default_column = *current_block.input_columns[
//                    workspace.argument_column_indices[2]].get();
//                to.insert(default_column[transform->current_row.row]);
//            }
//            else
//            {
//                to.insertDefault();
//            }
//        }
//        else
//        {
//            // Offset is inside the frame.
//            to.insertFrom(*transform->blockAt(target_row).input_columns[
//                              workspace.argument_column_indices[0]],
//                          target_row.row);
//        }
    }
};

void registerWindowFunctions(AggregateFunctionFactory & factory)
{
    factory.registerFunction(
        "rank",
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters) { return std::make_shared<WindowFunctionRank>(name, argument_types, parameters); },
        AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction(
        "dense_rank",
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters) { return std::make_shared<WindowFunctionDenseRank>(name, argument_types, parameters); },
        AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction(
        "row_number",
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters) { return std::make_shared<WindowFunctionRowNumber>(name, argument_types, parameters); },
        AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction(
        "lagInFrame",
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters) { return std::make_shared<WindowFunctionLagLeadInFrame<false>>(name, argument_types, parameters); },
        AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction(
        "leadInFrame",
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters) { return std::make_shared<WindowFunctionLagLeadInFrame<true>>(name, argument_types, parameters); },
        AggregateFunctionFactory::CaseInsensitive);
}


}