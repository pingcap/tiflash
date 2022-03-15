// Copyright 2022 PingCAP, Ltd.
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

#include <common/types.h>
#include <common/strong_typedef.h>

/** Represents number of days since 1970-01-01.
  * See DateLUTImpl for usage examples.
  */
STRONG_TYPEDEF(UInt16, DayNum)

/** Represent number of days since 1970-01-01 but in extended range,
 * for dates before 1970-01-01 and after 2105
 */
STRONG_TYPEDEF(Int32, ExtendedDayNum)
