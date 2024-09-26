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
    FIXED,
    // TODO support zipf and more distribution.
};

struct ColumnGeneratorOpts
{
    size_t size;
    String type_name;
    DataDistribution distribution;
    String name = ""; // NOLINT
    size_t string_max_size = 128;
    DataDistribution array_elems_distribution = DataDistribution::RANDOM;
    size_t array_elems_max_size = 10;
};

class ColumnGenerator : public ext::Singleton<ColumnGenerator>
{
public:
    ColumnWithTypeAndName generate(const ColumnGeneratorOpts & opts);

private:
    ColumnWithTypeAndName generateNullMapColumn(const ColumnGeneratorOpts & opts);
    std::mt19937_64 rand_gen;
    std::uniform_real_distribution<double> real_rand_gen;

    DataTypePtr createDecimalType();

    void genBool(MutableColumnPtr & col);
    template <typename IntegerType>
    void genInt(MutableColumnPtr & col);
    template <typename IntegerType>
    void genUInt(MutableColumnPtr & col);
    void genFloat(MutableColumnPtr & col);
    static void genString(MutableColumnPtr & col, UInt64 max_size);
    static void genDate(MutableColumnPtr & col);
    static void genDateTime(MutableColumnPtr & col);
    static void genDuration(MutableColumnPtr & col);
    void genDecimal(MutableColumnPtr & col, DataTypePtr & data_type);
    void genEnumValue(MutableColumnPtr & col, DataTypePtr & enum_type);
    void genVector(MutableColumnPtr & col, DataTypePtr & nested_type, size_t num_vals);
};
} // namespace DB::tests
