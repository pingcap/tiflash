#pragma once

#include <Core/Field.h>
#include <Core/Types.h>
#include <DataTypes/DataTypesNumber.h>

#include <memory>


namespace DB
{
class WindowBlockInputStream;
using WindowBlockInputStreamPtr = std::shared_ptr<WindowBlockInputStream>;

class WindowFunction
{
public:
    WindowFunction(const std::string & name_,
                   const DataTypes & argument_types_)
        : name(name_)
        , argument_types(argument_types_)
    {}

    String getName()
    {
        return name;
    }

    virtual ~WindowFunction() = default;

    virtual DataTypePtr getReturnType() const = 0;
    // Must insert the result for current_row.
    virtual void windowInsertResultInto(WindowBlockInputStreamPtr streamPtr,
                                        size_t function_index)
        = 0;

protected:
    std::string name;
    DataTypes argument_types;
};

using WindowFunctionPtr = std::shared_ptr<WindowFunction>;

} // namespace DB
