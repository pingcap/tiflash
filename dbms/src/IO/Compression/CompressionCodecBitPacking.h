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

namespace DB
{

class CompressionCodecBitPacking : public ICompressionCodec
{
public:
    explicit CompressionCodecBitPacking(UInt8 bytes_size_);

    UInt8 getMethodByte() const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size)
        const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; } // light compression
    bool isGenericCompression() const override { return false; }

private:
    enum class Mode : UInt8
    {
        Invalid = 0,
        AUTO = 1, // decide the best mode automatically
        CONSTANT = 2, // all values are the same
        CONSTANT_DELTA = 3, // the difference between two adjacent values is the same
        RLE = 4, // run-length encoding
        FOR = 5, // Frame of Reference encoding
        DELTA_FOR = 6, // delta encoding and then FOR encoding
        COPY = 7, // copy the original data
    };

    template <typename T>
    using ConstantState = T;

    template <typename T>
    using ConstantDeltaState = std::make_signed<T>::type;

    template <typename T>
    using RLEState = std::vector<std::pair<T, UInt16>>;

    template <typename T>
    struct FORState
    {
        T min_value;
        UInt8 bit_width;
    };

    template <typename T>
    struct DeltaFORState
    {
        std::make_signed<T>::type min_delta_value;
        UInt8 bit_width;
    };

    // State is a union of different states for different modes, like below:
    // template <typename T, typename TS = std::make_signed<T>::type>
    // Union State
    // {
    //     T constant; // for CONSTANT mode
    //     TS constant_delta; // for CONSTANT_DELTA mode
    //     std::vector<std::pair<T, UInt16>> rle_values; // for RLE mode
    //     T min_value; // for FOR mode
    //     TS min_delta_value; // for DELTA_FOR mode
    // };
    template <typename T>
    using State = std::variant<ConstantState<T>, ConstantDeltaState<T>, RLEState<T>, FORState<T>, DeltaFORState<T>>;

    template <typename T>
    Mode analyze(std::vector<T> & values, State<T> & state) const;

    template <typename T>
    size_t compressDataForType(const char * source, UInt32 source_size, char * dest) const;

    template <typename T>
    void decompressDataForType(const char * source, UInt32 source_size, char * dest, UInt32 output_size) const;

    Mode mode = Mode::AUTO;
    const UInt8 bytes_size;
    const UInt8 analyze_frequency = 1; // analyze every Nth call doCompressData, default is 1.
};

} // namespace DB
