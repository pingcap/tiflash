#include <Functions/FunctionFactory.h>

namespace DB
{
class FunctionFactory;

void registerFunctionPlus(FunctionFactory & factory);
void registerFunctionMinus(FunctionFactory & factory);
void registerFunctionMultiply(FunctionFactory & factory);
void registerFunctionDivideFloating(FunctionFactory & factory);
void registerFunctionTiDBDivideFloating(FunctionFactory & factory);
void registerFunctionDivideIntegral(FunctionFactory & factory);
void registerFunctionDivideIntegralOrZero(FunctionFactory & factory);
void registerFunctionModulo(FunctionFactory & factory);
void registerFunctionNegate(FunctionFactory & factory);
void registerFunctionAbs(FunctionFactory & factory);
void registerFunctionBitAnd(FunctionFactory & factory);
void registerFunctionBitOr(FunctionFactory & factory);
void registerFunctionBitXor(FunctionFactory & factory);
void registerFunctionBitNot(FunctionFactory & factory);
void registerFunctionBitShiftLeft(FunctionFactory & factory);
void registerFunctionBitShiftRight(FunctionFactory & factory);
void registerFunctionBitRotateLeft(FunctionFactory & factory);
void registerFunctionBitRotateRight(FunctionFactory & factory);
void registerFunctionLeast(FunctionFactory & factory);
void registerFunctionGreatest(FunctionFactory & factory);
void registerFunctionBitTest(FunctionFactory & factory);
void registerFunctionBitTestAny(FunctionFactory & factory);
void registerFunctionBitTestAll(FunctionFactory & factory);
void registerFunctionGCD(FunctionFactory & factory);
void registerFunctionLCM(FunctionFactory & factory);
void registerFunctionIntExp2(FunctionFactory & factory);
void registerFunctionIntExp10(FunctionFactory & factory);

void registerFunctionsArithmetic(FunctionFactory & factory)
{
    registerFunctionPlus(factory);
    registerFunctionMinus(factory);
    registerFunctionMultiply(factory);
    registerFunctionDivideFloating(factory);
    registerFunctionTiDBDivideFloating(factory);
    registerFunctionDivideIntegral(factory);
    registerFunctionDivideIntegralOrZero(factory);
    registerFunctionModulo(factory);
    registerFunctionNegate(factory);
    registerFunctionAbs(factory);
    registerFunctionBitAnd(factory);
    registerFunctionBitOr(factory);
    registerFunctionBitXor(factory);
    registerFunctionBitNot(factory);
    registerFunctionBitShiftLeft(factory);
    registerFunctionBitShiftRight(factory);
    registerFunctionBitRotateLeft(factory);
    registerFunctionBitRotateRight(factory);
    registerFunctionLeast(factory);
    registerFunctionGreatest(factory);
    registerFunctionBitTest(factory);
    registerFunctionBitTestAny(factory);
    registerFunctionBitTestAll(factory);
    registerFunctionGCD(factory);
    registerFunctionLCM(factory);
    registerFunctionIntExp2(factory);
    registerFunctionIntExp10(factory);
}

} // namespace DB
