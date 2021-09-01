#include <Functions/FunctionFactory.h>

#include "registerFunctionAbs.h"
#include "registerFunctionBitAnd.h"
#include "registerFunctionBitNot.h"
#include "registerFunctionBitOr.h"
#include "registerFunctionBitRotateLeft.h"
#include "registerFunctionBitRotateRight.h"
#include "registerFunctionBitShiftLeft.h"
#include "registerFunctionBitShiftRight.h"
#include "registerFunctionBitTest.h"
#include "registerFunctionBitTestAll.h"
#include "registerFunctionBitTestAny.h"
#include "registerFunctionBitXor.h"
#include "registerFunctionDivideFloating.h"
#include "registerFunctionDivideIntegral.h"
#include "registerFunctionDivideIntegralOrZero.h"
#include "registerFunctionGCD.h"
#include "registerFunctionGreatest.h"
#include "registerFunctionIntExp10.h"
#include "registerFunctionIntExp2.h"
#include "registerFunctionLCM.h"
#include "registerFunctionLeast.h"
#include "registerFunctionMinus.h"
#include "registerFunctionModulo.h"
#include "registerFunctionMultiply.h"
#include "registerFunctionNegate.h"
#include "registerFunctionPlus.h"
#include "registerFunctionTiDBDivideFloating.h"


namespace DB
{
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
