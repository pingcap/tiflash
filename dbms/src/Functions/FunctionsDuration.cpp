#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsDuration.h>
#include <Functions/IFunction.h>
#include <fmt/format.h>

namespace DB
{
DataTypePtr FunctionConvertDurationFromNanos::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    if (!arguments[0].type->isInteger())
    {
        throw Exception(
            fmt::format("Illegal type {} of first argument of function {}", arguments[0].type->getName(), getName()),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    if (!arguments[1].type->isInteger() || !arguments[1].column->isColumnConst())
    {
        throw Exception(
            fmt::format("Illegal type {} of second argument of function {}", arguments[1].type->getName(), getName()),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    auto fsp = arguments[1].column.get()->getInt(0);
    return std::make_shared<DataTypeMyDuration>(fsp);
}

void FunctionConvertDurationFromNanos::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const
{
    block.getByPosition(result).column = block.getByPosition(arguments[0]).column;
}

void registerFunctionsDuration(FunctionFactory & factory)
{
    factory.registerFunction<FunctionConvertDurationFromNanos>();
}
} // namespace DB
