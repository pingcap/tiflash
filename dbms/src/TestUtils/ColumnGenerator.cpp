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
    DataTypePtr type;
    if (opts.type_name == "Decimal")
        type = createDecimalType();
    else
        type = DataTypeFactory::instance().get(opts.type_name);

    auto col = type->createColumn();
    ColumnWithTypeAndName res({}, type, "", 0);
    String family_name = type->getFamilyName();

    for (size_t i = 0; i < opts.size; ++i)
    {
        if (family_name == "Int8" || family_name == "Int16" || family_name == "Int32" || family_name == "Int64")
            genInt(col);
        else if (family_name == "UInt8" || family_name == "UInt16" || family_name == "UInt32" || family_name == "UInt64")
            genUInt(col);
        else if (family_name == "Float32" || family_name == "Float64")
            genFloat(col);
        else if (family_name == "String")
            genString(col);
        else if (family_name == "MyDateTime")
            genDateTime(col);
        else if (family_name == "MyDate")
            genDate(col);
        else if (family_name == "Decimal")
            genDecimal(col, type);
    }

    res.column = std::move(col);
    return res;
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
    constexpr int size = 128;
    String str(size, 0);
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
    {
        s = s.substr(0, prec);
    }
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