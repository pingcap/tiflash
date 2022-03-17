#include <WindowFunctions/WindowFunctionFactory.h>

namespace DB
{
void registerWindowFunctions(WindowFunctionFactory & factory);

void registerWindowFunctions()
{
    auto & window_factory = WindowFunctionFactory::instance();
    registerWindowFunctions(window_factory);
}

} // namespace DB