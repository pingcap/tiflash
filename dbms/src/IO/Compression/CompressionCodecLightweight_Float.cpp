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

#include <Common/Exception.h>
#include <IO/Compression/ALP/Analyze.h>
#include <IO/Compression/CompressionCodecLightweight.h>
#include <IO/Compression/CompressionSettings.h>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_COMPRESS;
extern const int CANNOT_DECOMPRESS;
} // namespace ErrorCodes

template <typename T>
void CompressionCodecLightweight::FloatCompressContext::analyze(const std::span<const T> & values)
{
    if (!needAnalyze())
        return;

    auto & state = getState<T>();
    ALP::Analyze<T>::run(values, n_samples, state);
}

template void CompressionCodecLightweight::FloatCompressContext::analyze<float>(const std::span<const float> & values);
template void CompressionCodecLightweight::FloatCompressContext::analyze<double>(
    const std::span<const double> & values);

template <typename T>
size_t CompressionCodecLightweight::compressDataForFloat(const char * source, UInt32 source_size, char * dest) const
{
    char * dest_start = dest;

    const auto bytes_size = sizeof(T);
    if unlikely (source_size % bytes_size != 0)
        throw Exception(
            ErrorCodes::CANNOT_COMPRESS,
            "Cannot compress with lightweight-float codec, data size {} is not aligned to {}",
            source_size,
            bytes_size);

    // Load values
    const size_t count = source_size / bytes_size;
    std::span values(reinterpret_cast<const T *>(source), count);

    // Analyze
    float_ctx.analyze(values);

    // Compress
    auto & state = float_ctx.getState<T>();
    ALP::Compression<T, false>::compress(values.data(), count, state);

    // Store
    // Write header
    ALP::Constants::writeHeader(
        dest,
        state.frame_of_reference,
        state.bit_width,
        state.vector_encoding_indices.exponent,
        state.vector_encoding_indices.factor);
    // Write bitpacked values
    memcpy(dest, state.values_encoded.data(), state.bp_size);
    dest += state.bp_size;
    // Write exceptions
    if (!state.exceptions.empty())
    {
        memcpy(dest, state.exceptions.data(), state.exceptions.size() * sizeof(T));
        dest += state.exceptions.size() * sizeof(T);
        memcpy(dest, state.exceptions_positions.data(), state.exceptions_positions.size() * sizeof(UInt16));
        dest += state.exceptions_positions.size() * sizeof(UInt16);
    }

    return dest - dest_start;
}

template size_t CompressionCodecLightweight::compressDataForFloat<float>(
    const char * source,
    UInt32 source_size,
    char * dest) const;
template size_t CompressionCodecLightweight::compressDataForFloat<double>(
    const char * source,
    UInt32 source_size,
    char * dest) const;

template <typename T>
void CompressionCodecLightweight::decompressDataForFloat(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 output_size)
{
    if unlikely (output_size % sizeof(T) != 0)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress lightweight-float codec data. Uncompressed size {} is not aligned to {}",
            output_size,
            sizeof(T));

    size_t count = output_size / sizeof(T);
    // Read header
    auto [frame_of_reference, bit_width, exponent, factor] = ALP::Constants::readHeader(source);
    // Read bitpacked values
    auto bp_size = BitpackingPrimitives::getRequiredSize(count, bit_width);
    std::vector<UInt8> for_encoded(bp_size);
    memcpy(for_encoded.data(), source, bp_size);
    source += bp_size;
    // Read exceptions
    size_t exceptions_count
        = (source_size - (ALP::Constants::HEADER_SIZE + bp_size)) / (ALP::TypedConstants<T>::EXCEPTIONS_PAIR_SIZE);
    std::vector<T> exceptions(exceptions_count);
    std::vector<UInt16> exceptions_positions(exceptions_count);
    if (exceptions_count > 0)
    {
        memcpy(exceptions.data(), source, exceptions_count * sizeof(T));
        source += exceptions_count * sizeof(T);
        memcpy(exceptions_positions.data(), source, exceptions_count * sizeof(UInt16));
        source += exceptions_count * sizeof(UInt16);
    }
    // Decompress
    ALP::Decompression<T>::decompress(
        for_encoded.data(),
        reinterpret_cast<T *>(dest),
        count,
        factor,
        exponent,
        exceptions_count,
        exceptions.data(),
        exceptions_positions.data(),
        frame_of_reference,
        bit_width);
}

template void CompressionCodecLightweight::decompressDataForFloat<float>(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 output_size);
template void CompressionCodecLightweight::decompressDataForFloat<double>(
    const char * source,
    UInt32 source_size,
    char * dest,
    UInt32 output_size);

} // namespace DB
