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

#pragma once

#include <IO/Compression/ALP/Constants.h>
#include <common/types.h>

namespace DB::ALP
{

struct EncodingIndices
{
public:
    UInt8 exponent;
    UInt8 factor;

    EncodingIndices(UInt8 exponent, UInt8 factor)
        : exponent(exponent)
        , factor(factor)
    {}

    EncodingIndices()
        : exponent(0)
        , factor(0)
    {}
};

struct EncodingIndicesEquality
{
    bool operator()(const EncodingIndices & a, const EncodingIndices & b) const
    {
        return a.exponent == b.exponent && a.factor == b.factor;
    }
};

struct EncodingIndicesHash
{
    size_t operator()(const EncodingIndices & encoding_indices) const
    {
        using boost::hash_combine;
        using boost::hash_value;

        size_t seed = 0;
        hash_combine(seed, hash_value(encoding_indices.exponent));
        hash_combine(seed, hash_value(encoding_indices.factor));
        return seed;
    }
};

struct Combination
{
    EncodingIndices encoding_indices;
    UInt64 n_appearances;
    UInt64 estimated_compression_size;

    Combination(EncodingIndices encoding_indices, UInt64 n_appearances, UInt64 estimated_compression_size)
        : encoding_indices(encoding_indices)
        , n_appearances(n_appearances)
        , estimated_compression_size(estimated_compression_size)
    {}
};

template <class T>
class CompressionState
{
public:
    CompressionState()
        : vector_encoding_indices(0, 0)
        , bit_width(0)
        , bp_size(0)
        , frame_of_reference(0)
    {}

    void reset()
    {
        vector_encoding_indices = {0, 0};
        bit_width = 0;
    }

    void resetCombinations() { best_k_combinations.clear(); }

public:
    // The following variables will be the same for all vectors
    std::vector<Combination> best_k_combinations;

    // The following variables will be different for each vector
    EncodingIndices vector_encoding_indices; // Selected factor-exponent for the vector
    UInt8 bit_width; // Bit width for the bitpacking
    UInt64 bp_size; // Number of bytes after bitpacking
    UInt64 frame_of_reference; // Frame of reference for the vector
    std::vector<Int64> encoded_integers; // After ALP encoding
    std::vector<T> exceptions; // Exceptions
    std::vector<UInt16> exceptions_positions; // Positions of the exceptions
    std::vector<UInt8> values_encoded; // After bitpacking
};

} // namespace DB::ALP
