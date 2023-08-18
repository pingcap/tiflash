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

#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{
/** Get data type that covers intersection of all possible values of passed data types.
  * DataTypeNothing is the most common subtype for all types.
  * Examples: most common subtype for UInt16, UInt8 and Int8 - UInt16.
  * Examples: most common subtype for Array(UInt8), Int8 is Nothing
  *
  * If force_support_conversion is true, returns type which may be used to convert each argument to.
  * Example: most common subtype for Array(UInt8) and Array(Nullable(Int32)) is Array(Nullable(UInt8) if force_support_conversion is true.
  */
DataTypePtr getMostSubtype(
    const DataTypes & types,
    bool throw_if_result_is_nothing = false,
    bool force_support_conversion = false);

} // namespace DB
