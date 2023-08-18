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

/** exp10 from GNU libm fails to give precise result for integer arguments.
  * For example, exp10(3) gives 1000.0000000000001
  *  despite the fact that 1000 is exactly representable in double and float.
  * Better to always use implementation from MUSL.
  *
  * Note: the function names are different to avoid confusion with symbols from the system libm.
  */

double preciseExp10(double x);
