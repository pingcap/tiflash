#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsTiDBConversion.h>

namespace DB
{

String trim(const StringRef & value)
{
    StringRef ret;
    ret.size = 0;
    size_t start = 0;
    static std::unordered_set<char> spaces{'\t', '\n', '\v', '\f', '\r', ' '};
    for (; start < value.size; start++)
    {
        if (!spaces.count(value.data[start]))
            break;
    }
    size_t end = value.size;
    for (; start < end; end--)
    {
        if (!spaces.count(value.data[end - 1]))
            break;
    }
    if (start >= end)
        return ret.toString();
    ret.data = value.data + start;
    ret.size = end - start;
    return ret.toString();
}

void registerFunctionsTiDBConversion(FunctionFactory & factory) { factory.registerFunction<FunctionBuilderTiDBCast>(); }

} // namespace DB
