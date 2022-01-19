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

void registerWindowFunctions(AggregateFunctionFactory & factory);
}