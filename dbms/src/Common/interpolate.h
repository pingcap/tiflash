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

#include <cmath>


inline double interpolateLinear(double min, double max, double ratio)
{
    return min + (max - min) * ratio;
}


/** It is linear interpolation in logarithmic coordinates.
  * Exponential interpolation is related to linear interpolation
  *  exactly in same way as geometric mean is related to arithmetic mean.
  * 'min' must be greater than zero, 'ratio' must be from 0 to 1.
  */
inline double interpolateExponential(double min, double max, double ratio)
{
    return min * std::pow(max / min, ratio);
}
