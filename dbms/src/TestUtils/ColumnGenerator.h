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

#pragma once
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeDecimal.h>
#include <DataTypes/DataTypeFactory.h>

#include <ext/singleton.h>
#include <random>

namespace DB::tests
{
enum DataDistribution
{
    RANDOM,
    // TODO support zipf and more distribution.
};

struct ColumnGeneratorOpts
{
    size_t size;
    String type_name;
    DataDistribution distribution;
    String name = "";
    size_t string_max_size = 128;
};

class ColumnGenerator : public ext::Singleton<ColumnGenerator>
{
public:
    ColumnWithTypeAndName generate(const ColumnGeneratorOpts & opts);

private:
    ColumnWithTypeAndName generateNullMapColumn(const ColumnGeneratorOpts & opts);
    std::mt19937_64 rand_gen;
    std::uniform_int_distribution<Int64> int_rand_gen = std::uniform_int_distribution<Int64>(0, 128);
    std::uniform_real_distribution<double> real_rand_gen;
    const std::string charset{"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!@#$%^&*()、｜【】[]{}「」；：:;'‘,<《.>》。？·～`~"};

    String randomString();
    int randomTimeOffset();
    time_t randomUTCTimestamp();
    struct tm randomLocalTime();
    String randomDate();
    String randomDateTime();
    String randomDuration();
    String randomDecimal(uint64_t prec, uint64_t scale);

    DataTypePtr createDecimalType();

    void genBool(MutableColumnPtr & col);
    void genInt(MutableColumnPtr & col);
    void genUInt(MutableColumnPtr & col);
    void genFloat(MutableColumnPtr & col);
    void genString(MutableColumnPtr & col);
    void genDate(MutableColumnPtr & col);
    void genDateTime(MutableColumnPtr & col);
    void genDuration(MutableColumnPtr & col);
    void genDecimal(MutableColumnPtr & col, DataTypePtr & data_type);
    void genEnumValue(MutableColumnPtr & col, DataTypePtr & enum_type);
};
} // namespace DB::tests