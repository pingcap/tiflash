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

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <IO/Compression/CompressionFactory.h>
#include <IO/Compression/CompressionInfo.h>

#include <magic_enum.hpp>
#include <random>

namespace DB::tests
{

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

template <typename T, typename Generator>
CodecTestSequence generateSeq(Generator gen, const char * gen_name, int Begin = 0, int End = 10000)
{
    const auto direction = std::signbit(End - Begin) ? -1 : 1;
    std::vector<char> data(sizeof(T) * (End - Begin));
    char * write_pos = data.data();

    for (auto i = Begin; std::less<>{}(i, End); i += direction)
    {
        const T v = gen(static_cast<T>(i));

        unalignedStore<T>(write_pos, v);
        write_pos += sizeof(v);
    }

    return CodecTestSequence{
        (fmt::format("{} values of {} from {}", (End - Begin), type_name<T>(), gen_name)),
        std::move(data),
        makeDataType<T>(),
        sizeof(T)};
}

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

template <typename T>
struct RepeatGenerator
{
    explicit RepeatGenerator(T seed = 0, size_t min_repeat_count = 4, size_t max_repeat_count = 16)
        : random_engine(static_cast<std::uint_fast32_t>(seed))
        , value_distribution(std::numeric_limits<T>::min(), std::numeric_limits<T>::max())
        , repeat_distribution(min_repeat_count, max_repeat_count)
    {
        generate_next_value();
    }

    template <typename U>
    U operator()(U)
    {
        if (repeat_count == 0)
        {
            generate_next_value();
        }
        --repeat_count;
        return current_value;
    }

private:
    void generate_next_value()
    {
        current_value = value_distribution(random_engine);
        repeat_count = repeat_distribution(random_engine);
    }

    std::default_random_engine random_engine;
    std::uniform_int_distribution<T> value_distribution;
    std::uniform_int_distribution<size_t> repeat_distribution;
    T current_value;
    size_t repeat_count = 0;
};

} // namespace DB::tests
