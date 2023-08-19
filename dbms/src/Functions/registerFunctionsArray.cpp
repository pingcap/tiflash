// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArray.h>

#include "registerFunctionArray.h"
#include "registerFunctionArrayConcat.h"
#include "registerFunctionArrayElement.h"
#include "registerFunctionArrayEnumerate.h"
#include "registerFunctionArrayEnumerateUniq.h"
#include "registerFunctionArrayHasAll.h"
#include "registerFunctionArrayHasAny.h"
#include "registerFunctionArrayIntersect.h"
#include "registerFunctionArrayPopBack.h"
#include "registerFunctionArrayPopFront.h"
#include "registerFunctionArrayPushBack.h"
#include "registerFunctionArrayPushFront.h"
#include "registerFunctionArrayReduce.h"
#include "registerFunctionArrayResize.h"
#include "registerFunctionArrayReverse.h"
#include "registerFunctionArraySlice.h"
#include "registerFunctionArrayUniq.h"
#include "registerFunctionCountEqual.h"
#include "registerFunctionEmptyArrayDate.h"
#include "registerFunctionEmptyArrayDateTime.h"
#include "registerFunctionEmptyArrayFloat32.h"
#include "registerFunctionEmptyArrayFloat64.h"
#include "registerFunctionEmptyArrayInt16.h"
#include "registerFunctionEmptyArrayInt32.h"
#include "registerFunctionEmptyArrayInt64.h"
#include "registerFunctionEmptyArrayInt8.h"
#include "registerFunctionEmptyArrayString.h"
#include "registerFunctionEmptyArrayToSingle.h"
#include "registerFunctionEmptyArrayUInt16.h"
#include "registerFunctionEmptyArrayUInt32.h"
#include "registerFunctionEmptyArrayUInt64.h"
#include "registerFunctionEmptyArrayUInt8.h"
#include "registerFunctionHas.h"
#include "registerFunctionIndexOf.h"
#include "registerFunctionRange.h"


namespace DB
{
void registerFunctionsArray(FunctionFactory & factory)
{
    registerFunctionArray(factory);
    registerFunctionArrayElement(factory);
    registerFunctionHas(factory);
    registerFunctionIndexOf(factory);
    registerFunctionCountEqual(factory);
    registerFunctionArrayEnumerate(factory);
    registerFunctionArrayEnumerateUniq(factory);
    registerFunctionArrayUniq(factory);
    registerFunctionEmptyArrayUInt8(factory);
    registerFunctionEmptyArrayUInt16(factory);
    registerFunctionEmptyArrayUInt32(factory);
    registerFunctionEmptyArrayUInt64(factory);
    registerFunctionEmptyArrayInt8(factory);
    registerFunctionEmptyArrayInt16(factory);
    registerFunctionEmptyArrayInt32(factory);
    registerFunctionEmptyArrayInt64(factory);
    registerFunctionEmptyArrayFloat32(factory);
    registerFunctionEmptyArrayFloat64(factory);
    registerFunctionEmptyArrayDate(factory);
    registerFunctionEmptyArrayDateTime(factory);
    registerFunctionEmptyArrayString(factory);
    registerFunctionEmptyArrayToSingle(factory);
    registerFunctionRange(factory);
    registerFunctionArrayReduce(factory);
    registerFunctionArrayReverse(factory);
    registerFunctionArrayConcat(factory);
    registerFunctionArraySlice(factory);
    registerFunctionArrayPushBack(factory);
    registerFunctionArrayPushFront(factory);
    registerFunctionArrayPopBack(factory);
    registerFunctionArrayPopFront(factory);
    registerFunctionArrayHasAll(factory);
    registerFunctionArrayHasAny(factory);
    registerFunctionArrayIntersect(factory);
    registerFunctionArrayResize(factory);
}

} // namespace DB
