#include <Functions/registerFunctionBitRotateLeft.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsBinaryArithmetic.h>

namespace DB
{

void registerFunctionBitRotateLeft(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitRotateLeft>();
}

}
