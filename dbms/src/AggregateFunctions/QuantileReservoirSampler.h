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

#include <AggregateFunctions/ReservoirSampler.h>


namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

/** Quantile calculation with "reservoir sample" algorithm.
  * It collects pseudorandom subset of limited size from a stream of values,
  *  and approximate quantile from it.
  * The result is non-deterministic. Also look at QuantileReservoirSamplerDeterministic.
  *
  * This algorithm is quite inefficient in terms of precision for memory usage,
  *  but very efficient in CPU (though less efficient than QuantileTiming and than QuantileExact for small sets).
  */
template <typename Value>
struct QuantileReservoirSampler
{
    using Data = ReservoirSampler<Value, ReservoirSamplerOnEmpty::RETURN_NAN_OR_ZERO>;
    Data data;

    void add(const Value & x) { data.insert(x); }

    template <typename Weight>
    void add(const Value &, const Weight &)
    {
        throw Exception("Method add with weight is not implemented for ReservoirSampler", ErrorCodes::NOT_IMPLEMENTED);
    }

    void merge(const QuantileReservoirSampler & rhs) { data.merge(rhs.data); }

    void serialize(WriteBuffer & buf) const { data.write(buf); }

    void deserialize(ReadBuffer & buf) { data.read(buf); }

    /// Get the value of the `level` quantile. The level must be between 0 and 1.
    Value get(Float64 level) { return data.quantileInterpolated(level); }

    /// Get the `size` values of `levels` quantiles. Write `size` results starting with `result` address.
    /// indices - an array of index levels such that the corresponding elements will go in ascending order.
    void getMany(const Float64 * levels, const size_t * indices, size_t size, Value * result)
    {
        for (size_t i = 0; i < size; ++i)
            result[indices[i]] = data.quantileInterpolated(levels[indices[i]]);
    }

    /// The same, but in the case of an empty state, NaN is returned.
    Float64 getFloat(Float64 level) { return data.quantileInterpolated(level); }

    void getManyFloat(const Float64 * levels, const size_t * indices, size_t size, Float64 * result)
    {
        for (size_t i = 0; i < size; ++i)
            result[indices[i]] = data.quantileInterpolated(levels[indices[i]]);
    }
};

} // namespace DB
