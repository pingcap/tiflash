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

#include <AggregateFunctions/ReservoirSamplerDeterministic.h>


namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

/** Quantile calculation with "reservoir sample" algorithm.
  * It collects pseudorandom subset of limited size from a stream of values,
  *  and approximate quantile from it.
  * The function accept second argument, named "determinator"
  *  and a hash function from it is calculated and used as a source for randomness
  *  to apply random sampling.
  * The function is deterministic, but care should be taken with choose of "determinator" argument.
  */
template <typename Value>
struct QuantileReservoirSamplerDeterministic
{
    using Data = ReservoirSamplerDeterministic<Value, ReservoirSamplerDeterministicOnEmpty::RETURN_NAN_OR_ZERO>;
    Data data;

    void add(const Value &)
    {
        throw Exception(
            "Method add without determinator is not implemented for ReservoirSamplerDeterministic",
            ErrorCodes::NOT_IMPLEMENTED);
    }

    template <typename Determinator>
    void add(const Value & x, const Determinator & determinator)
    {
        data.insert(x, determinator);
    }

    void merge(const QuantileReservoirSamplerDeterministic & rhs) { data.merge(rhs.data); }

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
