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

#include <Common/BitpackingPrimitives.h>
#include <IO/Compression/ALP/CompressionState.h>

namespace DB::ALP
{

template <class T, bool EMPTY>
struct Compression
{
    using State = CompressionState<T>;
    static constexpr UInt8 EXACT_TYPE_BITSIZE = sizeof(T) * 8;

    /*
	 * Check for special values which are impossible for ALP to encode
	 * because they cannot be cast to int64 without an undefined behaviour
	 */
    static bool isImpossibleToEncode(T n)
    {
        return std::isinf(n) || std::isnan(n) || n > Constants::ENCODING_UPPER_LIMIT
            || n < Constants::ENCODING_LOWER_LIMIT || (n == 0.0 && std::signbit(n)); //! Verification for -0.0
    }

    /*
	 * Conversion from a Floating-Point number to Int64 without rounding
	 */
    static Int64 numberToInt64(T n)
    {
        if (isImpossibleToEncode(n))
        {
            return static_cast<Int64>(Constants::ENCODING_UPPER_LIMIT);
        }
        n = n + TypedConstants<T>::MAGIC_NUMBER - TypedConstants<T>::MAGIC_NUMBER;
        return static_cast<Int64>(n);
    }

    /*
	 * Encoding a single value with ALP
	 */
    static Int64 encodeValue(T value, EncodingIndices encoding_indices)
    {
        T tmp_encoded_value = value * TypedConstants<T>::EXP_ARR[encoding_indices.exponent]
            * TypedConstants<T>::FRAC_ARR[encoding_indices.factor];
        Int64 encoded_value = numberToInt64(tmp_encoded_value);
        return encoded_value;
    }

    /*
	 * Decoding a single value with ALP
	 */
    static T decodeValue(Int64 encoded_value, EncodingIndices encoding_indices)
    {
        //! The cast to T is needed to prevent a signed integer overflow
        T decoded_value = static_cast<T>(encoded_value) * Constants::FACT_ARR[encoding_indices.factor]
            * TypedConstants<T>::FRAC_ARR[encoding_indices.exponent];
        return decoded_value;
    }

    /*
	 * Return TRUE if c1 is a better combination than c2
	 * First criteria is number of times it appears as best combination
	 * Second criteria is the estimated compression size
	 * Third criteria is bigger exponent
	 * Fourth criteria is bigger factor
	 */
    static bool compareCombinations(const Combination & c1, const Combination & c2)
    {
        return (c1.n_appearances > c2.n_appearances)
            || (c1.n_appearances == c2.n_appearances && (c1.estimated_compression_size < c2.estimated_compression_size))
            || ((c1.n_appearances == c2.n_appearances && c1.estimated_compression_size == c2.estimated_compression_size)
                && (c2.encoding_indices.exponent < c1.encoding_indices.exponent))
            || ((c1.n_appearances == c2.n_appearances && c1.estimated_compression_size == c2.estimated_compression_size
                 && c2.encoding_indices.exponent == c1.encoding_indices.exponent)
                && (c2.encoding_indices.factor < c1.encoding_indices.factor));
    }

    /*
	 * Dry compress a vector (ideally a sample) to estimate ALP compression size given a exponent and factor
	 */
    template <bool PENALIZE_EXCEPTIONS>
    static UInt64 dryCompressToEstimateSize(const std::vector<T> & input_vector, EncodingIndices encoding_indices)
    {
        size_t n_values = input_vector.size();
        size_t exceptions_count = 0;
        size_t non_exceptions_count = 0;
        UInt32 estimated_bits_per_value = 0;
        UInt64 estimated_compression_size = 0;
        Int64 max_encoded_value = std::numeric_limits<Int64>::min();
        Int64 min_encoded_value = std::numeric_limits<Int64>::max();

        for (const T & value : input_vector)
        {
            Int64 encoded_value = encodeValue(value, encoding_indices);
            T decoded_value = decodeValue(encoded_value, encoding_indices);
            if (decoded_value == value)
            {
                non_exceptions_count++;
                max_encoded_value = std::max(encoded_value, max_encoded_value);
                min_encoded_value = std::min(encoded_value, min_encoded_value);
                continue;
            }
            exceptions_count++;
        }

        // We penalize combinations which yields to almost all exceptions
        if (PENALIZE_EXCEPTIONS && non_exceptions_count < 2)
        {
            return std::numeric_limits<UInt64>::max();
        }

        // Evaluate factor/exponent compression size (we optimize for FOR)
        UInt64 delta = (static_cast<UInt64>(max_encoded_value) - static_cast<UInt64>(min_encoded_value));
        estimated_bits_per_value = static_cast<UInt32>(std::ceil(std::log2(delta + 1)));
        estimated_compression_size += n_values * estimated_bits_per_value;
        estimated_compression_size
            += exceptions_count * (EXACT_TYPE_BITSIZE + (Constants::EXCEPTION_POSITION_SIZE * 8));
        return estimated_compression_size;
    }

    /*
	 * Find the best combinations of factor-exponent from each vector sampled from a rowgroup
	 * This function is called once per segment
	 * This operates over ALP first level samples
	 */
    static void findTopKCombinations(const std::vector<std::vector<T>> & vectors_sampled, State & state)
    {
        state.resetCombinations();

        std::unordered_map<EncodingIndices, UInt64, EncodingIndicesHash, EncodingIndicesEquality>
            best_k_combinations_hash;
        // For each vector sampled
        for (auto & sampled_vector : vectors_sampled)
        {
            size_t n_samples = sampled_vector.size();
            EncodingIndices best_encoding_indices = {TypedConstants<T>::MAX_EXPONENT, TypedConstants<T>::MAX_EXPONENT};

            //! We start our optimization with the worst possible total bits obtained from compression
            size_t best_total_bits = (n_samples * (EXACT_TYPE_BITSIZE + Constants::EXCEPTION_POSITION_SIZE * 8))
                + (n_samples * EXACT_TYPE_BITSIZE);

            // N of appearances is irrelevant at this phase; we search for the best compression for the vector
            Combination best_combination = {best_encoding_indices, 0, best_total_bits};
            //! We try all combinations in search for the one which minimize the compression size
            for (int8_t exp_idx = TypedConstants<T>::MAX_EXPONENT; exp_idx >= 0; exp_idx--)
            {
                for (int8_t factor_idx = exp_idx; factor_idx >= 0; factor_idx--)
                {
                    EncodingIndices current_encoding_indices
                        = {static_cast<UInt8>(exp_idx), static_cast<UInt8>(factor_idx)};
                    UInt64 estimated_compression_size
                        = dryCompressToEstimateSize<true>(sampled_vector, current_encoding_indices);
                    Combination current_combination = {current_encoding_indices, 0, estimated_compression_size};
                    if (compareCombinations(current_combination, best_combination))
                    {
                        best_combination = current_combination;
                    }
                }
            }
            best_k_combinations_hash[best_combination.encoding_indices]++;
        }

        // Convert our hash to a Combination vector to be able to sort
        // Note that this vector is always small (< 10 combinations)
        std::vector<Combination> best_k_combinations;
        best_k_combinations.reserve(best_k_combinations_hash.size());
        for (auto const & combination : best_k_combinations_hash)
        {
            best_k_combinations.emplace_back(
                combination.first, // Encoding Indices
                combination.second, // N of times it appeared (hash value)
                0 // Compression size is irrelevant at this phase since we compare combinations from different vectors
            );
        }
        std::sort(best_k_combinations.begin(), best_k_combinations.end(), compareCombinations);

        // Save k' best combinations
        for (size_t i = 0; i < std::min(Constants::MAX_COMBINATIONS, static_cast<UInt8>(best_k_combinations.size()));
             ++i)
        {
            state.best_k_combinations.push_back(best_k_combinations[i]);
        }
    }

    /*
	 * Find the best combination of factor-exponent for a vector from within the best k combinations
	 * This is ALP second level sampling
	 */
    static void findBestFactorAndExponent(const T * input_vector, size_t n_values, State & state)
    {
        //! We sample equidistant values within a vector; to do this we skip a fixed number of values
        std::vector<T> vector_sample;
        auto idx_increments = std::max<UInt32>(
            1,
            static_cast<UInt32>(std::ceil(static_cast<double>(n_values) / Constants::SAMPLES_PER_VECTOR)));
        for (size_t i = 0; i < n_values; i += idx_increments)
        {
            vector_sample.push_back(input_vector[i]);
        }

        EncodingIndices best_encoding_indices = {0, 0};
        UInt64 best_total_bits = std::numeric_limits<UInt64>::max();
        size_t worse_total_bits_counter = 0;

        //! We try each K combination in search for the one which minimize the compression size in the vector
        for (auto & combination : state.best_k_combinations)
        {
            UInt64 estimated_compression_size
                = dryCompressToEstimateSize<false>(vector_sample, combination.encoding_indices);

            // If current compression size is worse (higher) or equal than the current best combination
            if (estimated_compression_size >= best_total_bits)
            {
                worse_total_bits_counter += 1;
                // Early exit strategy
                if (worse_total_bits_counter == Constants::SAMPLING_EARLY_EXIT_THRESHOLD)
                {
                    break;
                }
                continue;
            }
            // Otherwise we replace the best and continue trying with the next combination
            best_total_bits = estimated_compression_size;
            best_encoding_indices = combination.encoding_indices;
            worse_total_bits_counter = 0;
        }
        state.vector_encoding_indices = best_encoding_indices;
    }

    /*
	 * ALP Compress
	 */
    static void compress(
        const T * input_vector,
        size_t n_values,
        const UInt16 * vector_null_positions,
        size_t nulls_count,
        State & state)
    {
        if (state.best_k_combinations.size() > 1)
        {
            findBestFactorAndExponent(input_vector, n_values, state);
        }
        else
        {
            state.vector_encoding_indices = state.best_k_combinations[0].encoding_indices;
        }

        state.encoded_integers.resize(n_values);
        state.exceptions.reserve(n_values);
        state.exceptions_positions.reserve(n_values);

        // Encoding Floating-Point to Int64
        //! We encode all the values regardless of their correctness to recover the original floating-point
        UInt64 min_encoded_value = std::numeric_limits<UInt64>::max();
        for (size_t i = 0; i < n_values; ++i)
        {
            T actual_value = input_vector[i];
            Int64 encoded_value = encodeValue(actual_value, state.vector_encoding_indices);
            T decoded_value = decodeValue(encoded_value, state.vector_encoding_indices);
            state.encoded_integers[i] = encoded_value;
            //! We detect exceptions using a predicated comparison
            if (decoded_value == actual_value)
            {
                min_encoded_value = std::min(min_encoded_value, static_cast<UInt64>(encoded_value));
                continue;
            }
            state.exceptions_positions.push_back(i);
        }

        // Replacing exceptions with the minimum encoded value
        for (size_t i = 0; i < state.exceptions_positions.size(); ++i)
        {
            size_t exception_pos = state.exceptions_positions[i];
            T actual_value = input_vector[exception_pos];
            state.encoded_integers[exception_pos] = min_encoded_value;
            state.exceptions.push_back(actual_value);
        }

        // Replacing nulls with that the non-exception-value
        for (size_t i = 0; i < nulls_count; i++)
        {
            UInt16 null_value_pos = vector_null_positions[i];
            state.encoded_integers[null_value_pos] = min_encoded_value;
        }

        // Analyze FFOR
        auto minmax = std::minmax_element(state.encoded_integers.begin(), state.encoded_integers.end());
        auto min_value = *minmax.first;
        auto max_value = *minmax.second;
        UInt64 min_max_diff = (static_cast<UInt64>(max_value) - static_cast<UInt64>(min_value));

        auto * u_encoded_integers = reinterpret_cast<UInt64 *>(state.encoded_integers.data());

        // Subtract FOR
        if (!EMPTY)
        { //! We only execute the FOR if we are writing the data
            for (size_t i = 0; i < n_values; ++i)
            {
                u_encoded_integers[i] -= static_cast<UInt64>(min_value);
            }
        }

        auto bit_width = BitpackingPrimitives::minimumBitWidth<UInt64, false>(min_max_diff);
        auto bp_size = BitpackingPrimitives::getRequiredSize(n_values, bit_width);
        if (!EMPTY && bit_width > 0)
        { //! We only execute the BP if we are writing the data
            state.values_encoded.reserve(bp_size);
            BitpackingPrimitives::packBuffer<UInt64, false>(
                state.values_encoded.data(),
                u_encoded_integers,
                n_values,
                bit_width);
        }
        state.bit_width = bit_width; // in bits
        state.bp_size = bp_size; // in bytes
        state.frame_of_reference = static_cast<UInt64>(min_value); // understood this can be negative
    }

    /*
	 * Overload without specifying nulls
	 */
    static void compress(const T * input_vector, size_t n_values, State & state)
    {
        compress(input_vector, n_values, nullptr, 0, state);
    }
};

template <class T>
struct Decompression
{
    static void decompress(
        UInt8 * for_encoded,
        T * output,
        size_t count,
        UInt8 vector_factor,
        UInt8 vector_exponent,
        UInt16 exceptions_count,
        T * exceptions,
        const UInt16 * exceptions_positions,
        UInt64 frame_of_reference,
        UInt8 bit_width)
    {
        EncodingIndices encoding_indices = {vector_exponent, vector_factor};

        // Bit Unpacking
        auto round_size = BitpackingPrimitives::roundUpToAlgorithmGroupSize(count);
        std::vector<UInt8> for_decoded(round_size * sizeof(T), 0);
        if (bit_width > 0)
        {
            BitpackingPrimitives::unPackBuffer<UInt64>(for_decoded.data(), for_encoded, count, bit_width);
        }
        auto * encoded_integers = reinterpret_cast<UInt64 *>(for_decoded.data());

        // unFOR
        for (size_t i = 0; i < count; i++)
        {
            encoded_integers[i] += frame_of_reference;
        }

        // Decoding
        for (size_t i = 0; i < count; i++)
        {
            auto encoded_integer = static_cast<Int64>(encoded_integers[i]);
            output[i] = Compression<T, true>::decodeValue(encoded_integer, encoding_indices);
        }

        // Exceptions Patching
        for (size_t i = 0; i < exceptions_count; ++i)
        {
            output[exceptions_positions[i]] = exceptions[i];
        }
    }
};

}; // namespace DB::ALP
