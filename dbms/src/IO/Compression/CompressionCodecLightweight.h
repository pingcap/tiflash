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

#include <IO/Compression/ICompressionCodec.h>

#include <span>


namespace DB
{

/**
 * @brief Lightweight compression codec
 * For integer data, it supports constant, constant delta, run-length, frame of reference, delta frame of reference, and LZ4.
 * For non-integer data, it supports LZ4.
 * The codec selects the best mode for each block of data.
 */
class CompressionCodecLightweight : public ICompressionCodec
{
public:
    explicit CompressionCodecLightweight(CompressionDataType data_type_);

    UInt8 getMethodByte() const override;

    ~CompressionCodecLightweight() override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size)
        const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; } // light compression
    bool isGenericCompression() const override { return false; }

private:
    /// Integer data

    enum class IntegerMode : UInt8
    {
        Invalid = 0,
        CONSTANT = 1, // all values are the same
        CONSTANT_DELTA = 2, // the difference between two adjacent values is the same
        RunLength = 3, // run-length encoding
        FOR = 4, // Frame of Reference encoding
        DELTA_FOR = 5, // delta encoding and then FOR encoding
        LZ4 = 6, // the above modes are not suitable, use LZ4 instead
    };

    // Constant or ConstantDelta
    template <typename T>
    using ConstantState = T;

    template <typename T>
    using RunLengthState = std::vector<std::pair<T, UInt8>>;

    template <typename T>
    struct FORState
    {
        std::vector<T> values;
        T min_value;
        UInt8 bit_width;
    };

    template <typename T>
    struct DeltaFORState
    {
        using TS = typename std::make_signed_t<T>;
        std::vector<TS> deltas;
        TS min_delta_value;
        UInt8 bit_width;
    };

    // State is a union of different states for different modes
    template <typename T>
    using IntegerState = std::variant<ConstantState<T>, RunLengthState<T>, FORState<T>, DeltaFORState<T>>;

    class IntegerCompressContext
    {
    public:
        IntegerCompressContext() = default;

        template <typename T>
        void analyze(std::span<const T> & values, IntegerState<T> & state);

        void update(size_t uncompressed_size, size_t compressed_size);

        String toDebugString() const;
        bool isCompression() const { return lz4_counter > 0 || lw_counter > 0; }

        IntegerMode mode = IntegerMode::LZ4;

    private:
        bool needAnalyze() const;
        bool needAnalyzeDelta() const;
        bool needAnalyzeRunLength() const;

    private:
        // The threshold for the number of blocks to decide whether need to analyze.
        // For example:
        // If lz4 is used more than COUNT_THRESHOLD times and the compression ratio is better than lightweight codec, do not analyze anymore.
        static constexpr size_t COUNT_THRESHOLD = 5;
        // Assume that the compression ratio of LZ4 is 3.0
        // The official document says that the compression ratio of LZ4 is 2.1, https://github.com/lz4/lz4
        static constexpr size_t ESRTIMATE_LZ4_COMPRESSION_RATIO = 3;

        size_t lw_uncompressed_size = 0;
        size_t lw_compressed_size = 0;
        size_t lw_counter = 0;
        size_t lz4_uncompressed_size = 0;
        size_t lz4_compressed_size = 0;
        size_t lz4_counter = 0;
        size_t constant_delta_counter = 0;
        size_t delta_for_counter = 0;
        size_t rle_counter = 0;
    };

    template <typename T>
    size_t compressDataForInteger(const char * source, UInt32 source_size, char * dest) const;

    template <typename T>
    void decompressDataForInteger(const char * source, UInt32 source_size, char * dest, UInt32 output_size) const;

    /// Non-integer data

    static size_t compressDataForNonInteger(const char * source, UInt32 source_size, char * dest);
    static void decompressDataForNonInteger(const char * source, UInt32 source_size, char * dest, UInt32 output_size);

private:
    mutable IntegerCompressContext ctx;
    const CompressionDataType data_type;
};

} // namespace DB
