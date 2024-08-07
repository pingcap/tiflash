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

#include <Common/PODArray.h>
#include <Common/config.h>
#include <IO/Compression/tests/CodecTestSequence.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>


namespace DB::tests
{

template <class T>
inline constexpr bool is_pod_v = std::is_trivial_v<std::is_standard_layout<T>>;

template <typename T, typename Container>
class BinaryDataAsSequenceOfValuesIterator
{
    const Container & container;
    const void * data;
    const void * data_end;

    T current_value;

public:
    using Self = BinaryDataAsSequenceOfValuesIterator<T, Container>;

    explicit BinaryDataAsSequenceOfValuesIterator(const Container & container_)
        : container(container_)
        , data(container.data())
        , data_end(container.data() + container.size())
        , current_value(T{})
    {
        static_assert(
            sizeof(container[0]) == 1 && is_pod_v<std::decay_t<decltype(container[0])>>,
            "Only works on containers of byte-size PODs.");
        read();
    }

    const T & operator*() const { return current_value; }

    size_t itemsLeft() const { return reinterpret_cast<const char *>(data_end) - reinterpret_cast<const char *>(data); }

    Self & operator++()
    {
        read();
        return *this;
    }

    explicit operator bool() const { return itemsLeft() > 0; }

private:
    void read()
    {
        if (!*this)
        {
            throw std::runtime_error("No more data to read");
        }

        current_value = unalignedLoad<T>(data);
        data = reinterpret_cast<const char *>(data) + sizeof(T);
    }
};

template <typename T, typename Container>
BinaryDataAsSequenceOfValuesIterator<T, Container> AsSequenceOf(const Container & container)
{
    return BinaryDataAsSequenceOfValuesIterator<T, Container>(container);
}

template <typename T, typename ContainerLeft, typename ContainerRight>
::testing::AssertionResult EqualByteContainersAs(const ContainerLeft & left, const ContainerRight & right)
{
    static_assert(sizeof(typename ContainerLeft::value_type) == 1, "Expected byte-container");
    static_assert(sizeof(typename ContainerRight::value_type) == 1, "Expected byte-container");

    ::testing::AssertionResult result = ::testing::AssertionSuccess();

    const auto l_size = left.size() / sizeof(T);
    const auto r_size = right.size() / sizeof(T);

    if (l_size != r_size)
    {
        result = ::testing::AssertionFailure() << "size mismatch, expected: " << l_size << " got:" << r_size;
    }
    if (l_size == 0 || r_size == 0)
    {
        return result;
    }

    auto l = AsSequenceOf<T>(left);
    auto r = AsSequenceOf<T>(right);

    while (l && r)
    {
        const auto left_value = *l;
        const auto right_value = *r;
        ++l;
        ++r;

        if (left_value != right_value)
        {
            if (result)
            {
                result = ::testing::AssertionFailure();
                break;
            }
        }
    }
    return result;
}

template <typename ContainerLeft, typename ContainerRight>
::testing::AssertionResult EqualByteContainers(
    uint8_t element_size,
    const ContainerLeft & left,
    const ContainerRight & right)
{
    switch (element_size)
    {
    case 1:
        return EqualByteContainersAs<UInt8>(left, right);
        break;
    case 2:
        return EqualByteContainersAs<UInt16>(left, right);
        break;
    case 4:
        return EqualByteContainersAs<UInt32>(left, right);
        break;
    case 8:
        return EqualByteContainersAs<UInt64>(left, right);
        break;
    default:
        assert(false && "Invalid element_size");
        return ::testing::AssertionFailure() << "Invalid element_size: " << element_size;
    }
}

CompressionCodecPtr makeCodec(const CompressionMethodByte method_byte, UInt8 type_byte)
{
    CompressionSetting setting(method_byte);
    setting.data_type = magic_enum::enum_cast<CompressionDataType>(type_byte).value();
    return CompressionFactory::create(setting);
}

void testTranscoding(ICompressionCodec & codec, const CodecTestSequence & test_sequence)
{
    const auto & source_data = test_sequence.serialized_data;

    const UInt32 encoded_max_size = codec.getCompressedReserveSize(static_cast<UInt32>(source_data.size()));
    PODArray<char> encoded(encoded_max_size);

    ASSERT_TRUE(source_data.data() != nullptr); // Codec assumes that source buffer is not null.
    const UInt32 encoded_size = codec.compress( //
        source_data.data(),
        static_cast<UInt32>(source_data.size()),
        encoded.data());
    encoded.resize(encoded_size);

    auto method_byte = ICompressionCodec::readMethod(encoded.data());
    ASSERT_EQ(method_byte, codec.getMethodByte());

    PODArray<char> decoded(source_data.size());
    const auto decode_codec = CompressionFactory::createForDecompress(method_byte);

    const auto decoded_size = decode_codec->readDecompressedBlockSize(encoded.data());
    decode_codec->decompress(encoded.data(), static_cast<UInt32>(encoded.size()), decoded.data(), decoded_size);
    decoded.resize(decoded_size);

    ASSERT_TRUE(EqualByteContainers(test_sequence.data_type->getSizeOfValueInMemory(), source_data, decoded));
}

class MultipleSequencesCodecTest
    : public ::testing::TestWithParam<std::tuple<CompressionMethodByte, std::vector<CodecTestSequence>>>
{
};

TEST_P(MultipleSequencesCodecTest, TranscodingWithDataType)
try
{
    const auto method_byte = std::get<0>(GetParam());
    const auto sequences = std::get<1>(GetParam());
    ASSERT_FALSE(sequences.empty());
    auto type_byte = sequences.front().type_byte;
    const auto codec = DB::tests::makeCodec(method_byte, type_byte);
    for (const auto & sequence : sequences)
    {
        ASSERT_EQ(sequence.type_byte, type_byte);
        DB::tests::testTranscoding(*codec, sequence);
    }
}
CATCH

class CodecTest : public ::testing::TestWithParam<std::tuple<CompressionMethodByte, CodecTestSequence>>
{
};

TEST_P(CodecTest, TranscodingWithDataType)
try
{
    const auto method_byte = std::get<0>(GetParam());
    const auto sequence = std::get<1>(GetParam());
    const auto codec = DB::tests::makeCodec(method_byte, sequence.type_byte);
    DB::tests::testTranscoding(*codec, sequence);
}
CATCH

// Makes many sequences with generator, first sequence length is 0, second is 1..., third is 2 up to `sequences_count`.
template <typename T, typename Generator>
std::vector<CodecTestSequence> generatePyramidOfSequences(
    const size_t sequences_count,
    Generator && generator,
    const char * generator_name)
{
    std::vector<CodecTestSequence> sequences;
    sequences.reserve(sequences_count);

    // Don't test against sequence of size 0, since it causes a nullptr source buffer as codec input and produces an error.
    // sequences.push_back(makeSeq<T>()); // sequence of size 0
    for (size_t i = 1; i < sequences_count; ++i)
    {
        std::string name = generator_name + std::string(" from 0 to ") + std::to_string(i);
        sequences.push_back(generateSeq<T>(std::forward<decltype(generator)>(generator), name.c_str(), 0, i));
    }

    return sequences;
}

// helper macro to produce human-friendly sequence name from generator
#define G(generator) generator, #generator

const auto IntegerCodecsToTest = ::testing::Values(
    CompressionMethodByte::Lightweight,
    CompressionMethodByte::DeltaFOR,
    CompressionMethodByte::FOR,
    CompressionMethodByte::RunLength
#if USE_QPL
    ,
    CompressionMethodByte::QPL
#endif
);

///////////////////////////////////////////////////////////////////////////////////////////////////
// test cases
///////////////////////////////////////////////////////////////////////////////////////////////////

INSTANTIATE_TEST_CASE_P(
    SmallSequences,
    MultipleSequencesCodecTest,
    ::testing::Combine(
        IntegerCodecsToTest,
        ::testing::Values(
            generatePyramidOfSequences<Int8>(42, G(SequentialGenerator(1))),
            generatePyramidOfSequences<Int16>(42, G(SequentialGenerator(1))),
            generatePyramidOfSequences<Int32>(42, G(SequentialGenerator(1))),
            generatePyramidOfSequences<Int64>(42, G(SequentialGenerator(1))),
            generatePyramidOfSequences<UInt8>(42, G(SequentialGenerator(1))),
            generatePyramidOfSequences<UInt16>(42, G(SequentialGenerator(1))),
            generatePyramidOfSequences<UInt32>(42, G(SequentialGenerator(1))),
            generatePyramidOfSequences<UInt64>(42, G(SequentialGenerator(1))))));

INSTANTIATE_TEST_CASE_P(
    Mixed,
    CodecTest,
    ::testing::Combine(
        IntegerCodecsToTest,
        ::testing::Values(
            generateSeq<Int8>(G(MinMaxGenerator()), 1, 5) + generateSeq<Int8>(G(SequentialGenerator(1)), 1, 1001),
            generateSeq<Int16>(G(MinMaxGenerator()), 1, 5) + generateSeq<Int16>(G(SequentialGenerator(1)), 1, 1001),
            generateSeq<Int32>(G(MinMaxGenerator()), 1, 5) + generateSeq<Int32>(G(SequentialGenerator(1)), 1, 1001),
            generateSeq<Int64>(G(MinMaxGenerator()), 1, 5) + generateSeq<Int64>(G(SequentialGenerator(1)), 1, 1001),
            generateSeq<UInt8>(G(MinMaxGenerator()), 1, 5) + generateSeq<UInt8>(G(SequentialGenerator(1)), 1, 1001),
            generateSeq<UInt16>(G(MinMaxGenerator()), 1, 5) + generateSeq<UInt16>(G(SequentialGenerator(1)), 1, 1001),
            generateSeq<UInt32>(G(MinMaxGenerator()), 1, 5) + generateSeq<UInt32>(G(SequentialGenerator(1)), 1, 1001),
            generateSeq<UInt64>(G(MinMaxGenerator()), 1, 5)
                + generateSeq<UInt64>(G(SequentialGenerator(1)), 1, 1001))));

INSTANTIATE_TEST_CASE_P(
    SameValueInt,
    CodecTest,
    ::testing::Combine(
        IntegerCodecsToTest,
        ::testing::Values(
            generateSeq<Int8>(G(SameValueGenerator(1000))),
            generateSeq<Int16>(G(SameValueGenerator(1000))),
            generateSeq<Int32>(G(SameValueGenerator(1000))),
            generateSeq<Int64>(G(SameValueGenerator(1000))),
            generateSeq<UInt8>(G(SameValueGenerator(1000))),
            generateSeq<UInt16>(G(SameValueGenerator(1000))),
            generateSeq<UInt32>(G(SameValueGenerator(1000))),
            generateSeq<UInt64>(G(SameValueGenerator(1000))))));

INSTANTIATE_TEST_CASE_P(
    SameNegativeValueInt,
    CodecTest,
    ::testing::Combine(
        IntegerCodecsToTest,
        ::testing::Values(
            generateSeq<Int8>(G(SameValueGenerator(-1000))),
            generateSeq<Int16>(G(SameValueGenerator(-1000))),
            generateSeq<Int32>(G(SameValueGenerator(-1000))),
            generateSeq<Int64>(G(SameValueGenerator(-1000))),
            generateSeq<UInt8>(G(SameValueGenerator(-1000))),
            generateSeq<UInt16>(G(SameValueGenerator(-1000))),
            generateSeq<UInt32>(G(SameValueGenerator(-1000))),
            generateSeq<UInt64>(G(SameValueGenerator(-1000))))));

INSTANTIATE_TEST_CASE_P(
    SequentialInt,
    CodecTest,
    ::testing::Combine(
        IntegerCodecsToTest,
        ::testing::Values(
            generateSeq<Int8>(G(SequentialGenerator(1))),
            generateSeq<Int16>(G(SequentialGenerator(1))),
            generateSeq<Int32>(G(SequentialGenerator(1))),
            generateSeq<Int64>(G(SequentialGenerator(1))),
            generateSeq<UInt8>(G(SequentialGenerator(1))),
            generateSeq<UInt16>(G(SequentialGenerator(1))),
            generateSeq<UInt32>(G(SequentialGenerator(1))),
            generateSeq<UInt64>(G(SequentialGenerator(1))))));

// -1, -2, -3, ... etc for signed
// 0xFF, 0xFE, 0xFD, ... for unsigned
INSTANTIATE_TEST_CASE_P(
    SequentialReverseInt,
    CodecTest,
    ::testing::Combine(
        IntegerCodecsToTest,
        ::testing::Values(
            generateSeq<Int8>(G(SequentialGenerator(-1))),
            generateSeq<Int16>(G(SequentialGenerator(-1))),
            generateSeq<Int32>(G(SequentialGenerator(-1))),
            generateSeq<Int64>(G(SequentialGenerator(-1))),
            generateSeq<UInt8>(G(SequentialGenerator(-1))),
            generateSeq<UInt16>(G(SequentialGenerator(-1))),
            generateSeq<UInt32>(G(SequentialGenerator(-1))),
            generateSeq<UInt64>(G(SequentialGenerator(-1))))));

INSTANTIATE_TEST_CASE_P(
    MonotonicInt,
    CodecTest,
    ::testing::Combine(
        IntegerCodecsToTest,
        ::testing::Values(
            generateSeq<Int8>(G(MonotonicGenerator(1, 5))),
            generateSeq<Int16>(G(MonotonicGenerator(1, 5))),
            generateSeq<Int32>(G(MonotonicGenerator(1, 5))),
            generateSeq<Int64>(G(MonotonicGenerator(1, 5))),
            generateSeq<UInt8>(G(MonotonicGenerator(1, 5))),
            generateSeq<UInt16>(G(MonotonicGenerator(1, 5))),
            generateSeq<UInt32>(G(MonotonicGenerator(1, 5))),
            generateSeq<UInt64>(G(MonotonicGenerator(1, 5))))));

INSTANTIATE_TEST_CASE_P(
    MonotonicReverseInt,
    CodecTest,
    ::testing::Combine(
        IntegerCodecsToTest,
        ::testing::Values(
            generateSeq<Int8>(G(MonotonicGenerator(-1, 5))),
            generateSeq<Int16>(G(MonotonicGenerator(-1, 5))),
            generateSeq<Int32>(G(MonotonicGenerator(-1, 5))),
            generateSeq<Int64>(G(MonotonicGenerator(-1, 5))),
            generateSeq<UInt8>(G(MonotonicGenerator(-1, 5))),
            generateSeq<UInt16>(G(MonotonicGenerator(-1, 5))),
            generateSeq<UInt32>(G(MonotonicGenerator(-1, 5))),
            generateSeq<UInt64>(G(MonotonicGenerator(-1, 5))))));

INSTANTIATE_TEST_CASE_P(
    RandomInt,
    CodecTest,
    ::testing::Combine(
        IntegerCodecsToTest,
        ::testing::Values(
            generateSeq<UInt8>(G(RandomGenerator<UInt8>(0))),
            generateSeq<UInt16>(G(RandomGenerator<UInt16>(0))),
            generateSeq<UInt32>(G(RandomGenerator<UInt32>(0, 0, 1000'000'000))),
            generateSeq<UInt64>(G(RandomGenerator<UInt64>(0, 0, 1000'000'000))))));


INSTANTIATE_TEST_CASE_P(
    RepeatInt,
    CodecTest,
    ::testing::Combine(
        IntegerCodecsToTest,
        ::testing::Values(
            generateSeq<UInt8>(G(RepeatGenerator<UInt8>(0))),
            generateSeq<UInt16>(G(RepeatGenerator<UInt16>(0))),
            generateSeq<UInt32>(G(RepeatGenerator<UInt32>(0))),
            generateSeq<UInt64>(G(RepeatGenerator<UInt64>(0))))));

// INSTANTIATE_TEST_CASE_P(
//     RandomishInt,
//     CodecTest,
//     ::testing::Combine(
//         IntegerCodecsToTest,
//         ::testing::Values(
//             generateSeq<Int32>(G(RandomishGenerator)),
//             generateSeq<Int64>(G(RandomishGenerator)),
//             generateSeq<UInt32>(G(RandomishGenerator)),
//             generateSeq<UInt64>(G(RandomishGenerator)),
//             generateSeq<Float32>(G(RandomishGenerator)),
//             generateSeq<Float64>(G(RandomishGenerator)))));


// INSTANTIATE_TEST_CASE_P(
//     RandomishFloat,
//     CodecTest,
//     ::testing::Combine(
//         IntegerCodecsToTest,
//         ::testing::Values(generateSeq<Float32>(G(RandomishGenerator)), generateSeq<Float64>(G(RandomishGenerator)))));

} // namespace DB::tests
