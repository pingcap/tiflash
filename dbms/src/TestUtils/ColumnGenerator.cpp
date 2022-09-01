// Copyright 2022 PingCAP, Ltd.
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
#include <TestUtils/ColumnGenerator.h>

namespace DB::tests
{
ColumnWithTypeAndName ColumnGenerator::generate(const ColumnGeneratorOpts & opts)
{
    int_rand_gen = std::uniform_int_distribution<Int64>(0, opts.string_max_size);
    DataTypePtr type;
    if (opts.type_name == "Decimal")
        type = createDecimalType();
    else
        type = DataTypeFactory::instance().get(opts.type_name);

    auto col = type->createColumn();
    col->reserve(opts.size);

    auto type_id = type->getTypeId();

    switch (type_id)
    {
    case TypeIndex::UInt8:
    case TypeIndex::UInt16:
    case TypeIndex::UInt32:
    case TypeIndex::UInt64:
        for (size_t i = 0; i < opts.size; ++i)
            genUInt(col);
        break;
    case TypeIndex::Int8:
    case TypeIndex::Int16:
    case TypeIndex::Int32:
    case TypeIndex::Int64:
        for (size_t i = 0; i < opts.size; ++i)
            genInt(col);
        break;
    case TypeIndex::Float32:
    case TypeIndex::Float64:
        for (size_t i = 0; i < opts.size; ++i)
            genFloat(col);
        break;
    case TypeIndex::String:
        for (size_t i = 0; i < opts.size; ++i)
            genString(col);
        break;
    case TypeIndex::Decimal32:
    case TypeIndex::Decimal64:
    case TypeIndex::Decimal128:
    case TypeIndex::Decimal256:
        for (size_t i = 0; i < opts.size; ++i)
            genDecimal(col, type);
        break;
    case TypeIndex::MyDate:
        for (size_t i = 0; i < opts.size; ++i)
            genDate(col);
        break;
    case TypeIndex::MyDateTime:
        for (size_t i = 0; i < opts.size; ++i)
            genDateTime(col);
        break;
    default:
        throw std::invalid_argument("RandomColumnGenerator invalid type");
    }

    return {std::move(col), type};
}

DataTypePtr ColumnGenerator::createDecimalType()
{
    static const int max_precision = std::to_string(std::numeric_limits<uint64_t>::max()).size();
    int prec = rand_gen() % max_precision + 1;
    int scale = rand_gen() % prec;
    return DB::createDecimal(prec, scale);
}

String ColumnGenerator::randomString()
{
    String str(int_rand_gen(rand_gen), 0);
    std::generate_n(str.begin(), str.size(), [this]() { return charset[rand_gen() % charset.size()]; });
    return str;
}

int ColumnGenerator::randomTimeOffset()
{
    static constexpr int max_offset = 24 * 3600 * 10000; // 10000 days for test
    return (rand_gen() % max_offset) * (rand_gen() % 2 == 0 ? 1 : -1);
}

time_t ColumnGenerator::randomUTCTimestamp()
{
    return ::time(nullptr) + randomTimeOffset();
}

struct tm ColumnGenerator::randomLocalTime()
{
    time_t t = randomUTCTimestamp();
    struct tm res
    {
    };

    if (localtime_r(&t, &res) == nullptr)
    {
        throw std::invalid_argument(fmt::format("localtime_r({}) ret {}", t, strerror(errno)));
    }
    return res;
}

String ColumnGenerator::randomDate()
{
    auto res = randomLocalTime();
    return fmt::format("{}-{}-{}", res.tm_year + 1900, res.tm_mon + 1, res.tm_mday);
}

String ColumnGenerator::randomDateTime()
{
    auto res = randomLocalTime();
    return fmt::format("{}-{}-{} {}:{}:{}", res.tm_year + 1900, res.tm_mon + 1, res.tm_mday, res.tm_hour, res.tm_min, res.tm_sec);
}

String ColumnGenerator::randomDecimal(uint64_t prec, uint64_t scale)
{
    auto s = std::to_string(rand_gen());
    if (s.size() < prec)
        s += String(prec - s.size(), '0');
    else if (s.size() > prec)
        s = s.substr(0, prec);
    return s.substr(0, prec - scale) + "." + s.substr(prec - scale);
}

void ColumnGenerator::genInt(MutableColumnPtr & col)
{
    Field f = static_cast<Int64>(rand_gen());
    col->insert(f);
}

void ColumnGenerator::genUInt(MutableColumnPtr & col)
{
    Field f = static_cast<UInt64>(rand_gen());
    col->insert(f);
}

void ColumnGenerator::genFloat(MutableColumnPtr & col)
{
    Field f = static_cast<Float64>(real_rand_gen(rand_gen));
    col->insert(f);
}

void ColumnGenerator::genString(MutableColumnPtr & col)
{
    Field f = randomString();
    col->insert(f);
}

void ColumnGenerator::genDate(MutableColumnPtr & col)
{
    Field f = parseMyDateTime(randomDate());
    col->insert(f);
}

void ColumnGenerator::genDateTime(MutableColumnPtr & col)
{
    Field f = parseMyDateTime(randomDateTime());
    col->insert(f);
}

void ColumnGenerator::genDecimal(MutableColumnPtr & col, DataTypePtr & data_type)
{
    auto prec = getDecimalPrecision(*data_type, 0);
    auto scale = getDecimalScale(*data_type, 0);
    auto s = randomDecimal(prec, scale);
    bool negative = rand_gen() % 2 == 0;
    Field f;
    if (parseDecimal(s.data(), s.size(), negative, f))
    {
        col->insert(f);
    }
    else
    {
        throw std::invalid_argument(fmt::format("RandomColumnGenerator parseDecimal({}, {}) prec {} scale {} fail", s, negative, prec, scale));
    }
}
} // namespace DB::tests