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

#include <common/types.h>

/** Almost the same as x = x * exp10(exponent), but gives more accurate result.
  * Example:
  *  5 * 1e-11 = 4.9999999999999995e-11
  *  !=
  *  5e-11 = shift10(5.0, -11)
  */

double shift10(double x, int exponent);
float shift10(float x, int exponent);

double shift10(UInt64 x, int exponent);
double shift10(Int64 x, int exponent);
