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

#include <Common/FieldVisitors.h>
#include <Common/NaNUtils.h>
#include <Core/Field.h>

#include <vector>


namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int PARAMETER_OUT_OF_BOUND;
} // namespace ErrorCodes


/** Parameters of different functions quantiles*.
  * - list of levels of quantiles.
  * It is also necessary to calculate an array of indices of levels that go in ascending order.
  *
  * Example: quantiles(0.5, 0.99, 0.95)(x).
  * levels: 0.5, 0.99, 0.95
  * levels_permutation: 0, 2, 1
  */
template <typename T> /// float or double
struct QuantileLevels
{
    using Levels = std::vector<T>;
    using Permutation = std::vector<size_t>;

    Levels levels;
    Permutation permutation; /// Index of the i-th level in `levels`.

    size_t size() const { return levels.size(); }

    QuantileLevels(const Array & params, bool require_at_least_one_param)
    {
        if (params.empty())
        {
            if (require_at_least_one_param)
                throw Exception(
                    "Aggregate function for calculation of multiple quantiles require at least one parameter",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            /// If levels are not specified, default is 0.5 (median).
            levels.push_back(0.5);
            permutation.push_back(0);
            return;
        }

        size_t size = params.size();
        levels.resize(size);
        permutation.resize(size);

        for (size_t i = 0; i < size; ++i)
        {
            levels[i] = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[i]);

            if (isNaN(levels[i]) || levels[i] < 0 || levels[i] > 1)
                throw Exception("Quantile level is out of range [0..1]", ErrorCodes::PARAMETER_OUT_OF_BOUND);

            permutation[i] = i;
        }

        std::sort(permutation.begin(), permutation.end(), [this](size_t a, size_t b) { return levels[a] < levels[b]; });
    }
};


} // namespace DB
