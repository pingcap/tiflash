#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/ColumnNumbers.h>
#include <Core/Names.h>


namespace DB
{
struct AggregateDescription
{
    AggregateFunctionPtr function;
    Array parameters; /// Parameters of the (parametric) aggregate function.
    ColumnNumbers arguments;
    Names argument_names; /// used if no `arguments` are specified.
    String column_name; /// What name to use for a column with aggregate function values
};

using AggregateDescriptions = std::vector<AggregateDescription>;

} // namespace DB
