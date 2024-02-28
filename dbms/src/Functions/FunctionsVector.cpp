// Copyright 2024 PingCAP, Inc.
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

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsVector.h>
#include <TiDB/Decode/Vector.h>

#include <memory>

namespace DB
{

void registerFunctionsVector(FunctionFactory & factory)
{
    factory.registerFunction<FunctionsCastVectorFloat32AsString>();
    factory.registerFunction<FunctionsCastVectorFloat32AsVectorFloat32>();
    factory.registerFunction<FunctionsVecAsText>();
    factory.registerFunction<FunctionsVecDims>();
    factory.registerFunction<FunctionsVecL1Distance>();
    factory.registerFunction<FunctionsVecL2Distance>();
    factory.registerFunction<FunctionsVecL2Norm>();
    factory.registerFunction<FunctionsVecCosineDistance>();
    factory.registerFunction<FunctionsVecNegativeInnerProduct>();
}

} // namespace DB
