#pragma once

#include <Core/Field.h>
#include <Core/Types.h>
#include <WindowFunctions/IWindowFunction.h>

#include <ext/singleton.h>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB
{
class Context;
class IDataType;

using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;

/** Creates an window function by name.
  */
class WindowFunctionFactory final : public ext::Singleton<WindowFunctionFactory>
{
public:
    using Creator = std::function<WindowFunctionPtr(const String &, const DataTypes &)>;

    /// Register a function by its name.
    /// No locking, you must register all functions before usage of get.
    void registerFunction(
        const String & name,
        Creator creator);

    /// Throws an exception if not found.
    WindowFunctionPtr get(
        const String & name,
        const DataTypes & argument_types) const;

    /// Returns nullptr if not found.
    WindowFunctionPtr tryGet(const String & name, const DataTypes & argument_types) const;

    bool isWindowFunctionName(const String & name) const;

private:
    WindowFunctionPtr getImpl(
        const String & name,
        const DataTypes & argument_types) const;

private:
    using WindowFunctions = std::unordered_map<String, Creator>;

    WindowFunctions window_functions;
};

} // namespace DB
