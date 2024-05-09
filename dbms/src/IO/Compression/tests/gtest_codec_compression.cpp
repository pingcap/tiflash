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

#include <Common/PODArray.h>
#include <Common/config.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <IO/Compression/CompressionFactory.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/types.h>
#include <gtest/gtest.h>

#include <magic_enum.hpp>
#include <random>


namespace DB::tests
{

template <class T>
inline constexpr bool is_pod_v = std::is_trivial_v<std::is_standard_layout<T>>;

template <typename T>
struct AsHexStringHelper
{
    const T & container;
};

template <typename T>
std::ostream & operator<<(std::ostream & ostr, const AsHexStringHelper<T> & helper)
{
    ostr << std::hex;
    for (const auto & e : helper.container)
    {
        ostr << "\\x" << std::setw(2) << std::setfill('0') << (static_cast<unsigned int>(e) & 0xFF);
    }

    return ostr;
}

template <typename T>
AsHexStringHelper<T> AsHexString(const T & container)
{
    static_assert(
        sizeof(container[0]) == 1 && is_pod_v<std::decay_t<decltype(container[0])>>,
        "Only works on containers of byte-size PODs.");

    return AsHexStringHelper<T>{container};
}

template <typename T>
std::string bin(const T & value, size_t bits = sizeof(T) * 8)
{
    static const uint8_t MAX_BITS = sizeof(T) * 8;
    assert(bits <= MAX_BITS);

    return std::bitset<sizeof(T) * 8>(static_cast<uint64_t>(value)).to_string().substr(MAX_BITS - bits, bits);
}

template <typename T>
const char * type_name()
{
#define MAKE_TYPE_NAME(TYPE)               \
    if constexpr (std::is_same_v<TYPE, T>) \
    return #TYPE

    MAKE_TYPE_NAME(UInt8);
    MAKE_TYPE_NAME(UInt16);
    MAKE_TYPE_NAME(UInt32);
    MAKE_TYPE_NAME(UInt64);
    MAKE_TYPE_NAME(Int8);
    MAKE_TYPE_NAME(Int16);
    MAKE_TYPE_NAME(Int32);
    MAKE_TYPE_NAME(Int64);
    MAKE_TYPE_NAME(Float32);
    MAKE_TYPE_NAME(Float64);

#undef MAKE_TYPE_NAME

    return typeid(T).name();
}

template <typename T>
DataTypePtr makeDataType()
{
#define MAKE_DATA_TYPE(TYPE)               \
    if constexpr (std::is_same_v<T, TYPE>) \
    return std::make_shared<DataType##TYPE>()

    MAKE_DATA_TYPE(UInt8);
    MAKE_DATA_TYPE(UInt16);
    MAKE_DATA_TYPE(UInt32);
    MAKE_DATA_TYPE(UInt64);
    MAKE_DATA_TYPE(Int8);
    MAKE_DATA_TYPE(Int16);
    MAKE_DATA_TYPE(Int32);
    MAKE_DATA_TYPE(Int64);
    MAKE_DATA_TYPE(Float32);
    MAKE_DATA_TYPE(Float64);

#undef MAKE_DATA_TYPE

    assert(false && "unknown datatype");
    return nullptr;
}

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


struct CodecTestSequence
{
    std::string name;
    std::vector<char> serialized_data;
    DataTypePtr data_type;
    UInt8 type_byte;

    CodecTestSequence(std::string name_, std::vector<char> serialized_data_, DataTypePtr data_type_, UInt8 type_byte_)
        : name(name_)
        , serialized_data(serialized_data_)
        , data_type(data_type_)
        , type_byte(type_byte_)
    {}

    CodecTestSequence & append(const CodecTestSequence & other)
    {
        assert(data_type->equals(*other.data_type));

        serialized_data.insert(serialized_data.end(), other.serialized_data.begin(), other.serialized_data.end());
        if (!name.empty())
            name += " + ";
        name += other.name;

        return *this;
    }
};

CodecTestSequence operator+(CodecTestSequence && left, const CodecTestSequence & right)
{
    return left.append(right);
}

template <typename T>
CodecTestSequence operator*(CodecTestSequence && left, T times)
{
    std::vector<char> data(std::move(left.serialized_data));
    const size_t initial_size = data.size();
    const size_t final_size = initial_size * times;

    data.reserve(final_size);

    for (T i = 0; i < times; ++i)
    {
        data.insert(data.end(), data.begin(), data.begin() + initial_size);
    }

    return CodecTestSequence{
        left.name + " x " + std::to_string(times),
        std::move(data),
        std::move(left.data_type),
        sizeof(T)};
}

std::ostream & operator<<(std::ostream & ostr, const CompressionMethodByte method_byte)
{
    ostr << "Codec{name: " << magic_enum::enum_name(method_byte) << "}";
    return ostr;
}

std::ostream & operator<<(std::ostream & ostr, const CodecTestSequence & seq)
{
    return ostr << "CodecTestSequence{"
                << "name: " << seq.name << ", type name: " << seq.data_type->getName()
                << ", data size: " << seq.serialized_data.size() << " bytes"
                << "}";
}

template <typename T, typename... Args>
CodecTestSequence makeSeq(Args &&... args)
{
    std::initializer_list<T> vals{static_cast<T>(args)...};
    std::vector<char> data(sizeof(T) * std::size(vals));

    char * write_pos = data.data();
    for (const auto & v : vals)
    {
        unalignedStore<T>(write_pos, v);
        write_pos += sizeof(v);
    }

    return CodecTestSequence{
        (fmt::format("{} values of {}", std::size(vals), type_name<T>())),
        std::move(data),
        makeDataType<T>(),
        sizeof(T)};
}

template <typename T, typename Generator, typename B = int, typename E = int>
CodecTestSequence generateSeq(Generator gen, const char * gen_name, B Begin = 0, E End = 10000)
{
    const auto direction = std::signbit(End - Begin) ? -1 : 1;
    std::vector<char> data(sizeof(T) * (End - Begin));
    char * write_pos = data.data();

    for (auto i = Begin; std::less<>{}(i, End); i += direction)
    {
        const T v = static_cast<T>(gen(i));

        unalignedStore<T>(write_pos, v);
        write_pos += sizeof(v);
    }

    return CodecTestSequence{
        (fmt::format("{} values of {} from {}", (End - Begin), type_name<T>(), gen_name)),
        std::move(data),
        makeDataType<T>(),
        sizeof(T)};
}

CompressionCodecPtr makeCodec(const CompressionMethodByte method_byte, UInt8 type_byte)
{
    CompressionSetting setting(method_byte);
    setting.type_bytes_size = type_byte;
    return CompressionFactory::create(setting);
}

void testTranscoding(ICompressionCodec & codec, const CodecTestSequence & test_sequence)
{
    const auto & source_data = test_sequence.serialized_data;

    const UInt32 encoded_max_size = codec.getCompressedReserveSize(static_cast<UInt32>(source_data.size()));
    PODArray<char> encoded(encoded_max_size);

    assert(source_data.data() != nullptr); // Codec assumes that source buffer is not null.
    const UInt32 encoded_size
        = codec.compress(source_data.data(), static_cast<UInt32>(source_data.size()), encoded.data());

    encoded.resize(encoded_size);

    PODArray<char> decoded(source_data.size());

    const auto decoded_size = codec.readDecompressedBlockSize(encoded.data());

    codec.decompress(encoded.data(), static_cast<UInt32>(encoded.size()), decoded.data(), decoded_size);

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

///////////////////////////////////////////////////////////////////////////////////////////////////
// Here we use generators to produce test payload for codecs.
// Generator is a callable that can produce infinite number of values,
// output value MUST be of the same type as input value.
///////////////////////////////////////////////////////////////////////////////////////////////////

auto SameValueGenerator = [](auto value) {
    return [=](auto i) {
        return static_cast<decltype(i)>(value);
    };
};

auto SequentialGenerator = [](auto stride = 1) {
    return [=](auto i) {
        using ValueType = decltype(i);
        return static_cast<ValueType>(stride * i);
    };
};

template <typename T>
using uniform_distribution = typename std::conditional_t<
    std::is_floating_point_v<T>,
    std::uniform_real_distribution<T>,
    typename std::conditional_t<is_integer_v<T>, std::uniform_int_distribution<T>, void>>;


template <typename T = Int32>
struct MonotonicGenerator
{
    explicit MonotonicGenerator(T stride_ = 1, T max_step = 10)
        : prev_value(0)
        , stride(stride_)
        , random_engine(0)
        , distribution(0, max_step)
    {}

    template <typename U>
    U operator()(U)
    {
        prev_value = prev_value + stride * distribution(random_engine);
        return static_cast<U>(prev_value);
    }

private:
    T prev_value;
    const T stride;
    std::default_random_engine random_engine;
    uniform_distribution<T> distribution;
};

template <typename T>
struct RandomGenerator
{
    explicit RandomGenerator(
        T seed = 0,
        T value_min = std::numeric_limits<T>::min(),
        T value_max = std::numeric_limits<T>::max())
        : random_engine(static_cast<uint_fast32_t>(seed))
        , distribution(value_min, value_max)
    {}

    template <typename U>
    U operator()(U)
    {
        return static_cast<U>(distribution(random_engine));
    }

private:
    std::default_random_engine random_engine;
    uniform_distribution<T> distribution;
};

// auto RandomishGenerator = [](auto i) {
//     using T = decltype(i);
//     double sin_value = sin(static_cast<double>(i * i)) * i;
//     if (sin_value < std::numeric_limits<T>::lowest() || sin_value > static_cast<double>(std::numeric_limits<T>::max()))
//         return T{};
//     return static_cast<T>(sin_value);
// };

auto MinMaxGenerator = []() {
    return [step = 0](auto i) mutable {
        if (step++ % 2 == 0)
        {
            return std::numeric_limits<decltype(i)>::min();
        }
        else
        {
            return std::numeric_limits<decltype(i)>::max();
        }
    };
};

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
    // CompressionMethodByte::FOR, // disable FOR codec for now, since there are too many unit tests.
    CompressionMethodByte::RunLength
#if USE_QPL
    ,
    CompressionMethodByte::QPL
#endif
);

///////////////////////////////////////////////////////////////////////////////////////////////////
// test cases
///////////////////////////////////////////////////////////////////////////////////////////////////

// INSTANTIATE_TEST_CASE_P(
//     Simple,
//     CodecTest,
//     ::testing::Combine(
//         IntegerCodecsToTest,
//         ::testing::Values(makeSeq<Float64>(
//             1,
//             2,
//             3,
//             5,
//             7,
//             11,
//             13,
//             17,
//             23,
//             29,
//             31,
//             37,
//             41,
//             43,
//             47,
//             53,
//             59,
//             61,
//             67,
//             71,
//             73,
//             79,
//             83,
//             89,
//             97))));

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
