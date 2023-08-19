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

#include <DataTypes/DataTypeEnum.h>
#include <Storages/DeltaMerge/workload/DataGenerator.h>
#include <Storages/DeltaMerge/workload/KeyGenerator.h>
#include <Storages/DeltaMerge/workload/Options.h>
#include <Storages/DeltaMerge/workload/TableGenerator.h>
#include <Storages/DeltaMerge/workload/TimestampGenerator.h>
#include <fmt/ranges.h>

#include <random>

namespace DB::DM::tests
{
class RandomDataGenerator : public DataGenerator
{
public:
    RandomDataGenerator(const TableInfo & table_info_, TimestampGenerator & ts_gen_)
        : table_info(table_info_)
        , ts_gen(ts_gen_)
        , rand_gen(std::random_device()())
    {}

    std::tuple<Block, uint64_t> get(uint64_t key) override
    {
        Block block;
        // Generate 'rowkeys'.
        // Currently not support common handle and rowkey is handle column.
        for (int i : table_info.rowkey_column_indexes)
        {
            auto & col_def = (*table_info.columns)[i];
            ColumnWithTypeAndName col({}, col_def.type, col_def.name, col_def.id);
            IColumn::MutablePtr mut_col = col.type->createColumn();
            std::string family_name = col.type->getFamilyName();
            if (family_name == "Int8" || family_name == "Int16" || family_name == "Int32" || family_name == "Int64")
            {
                Field f = static_cast<Int64>(key);
                mut_col->insert(f);
            }
            else if (family_name == "UInt8" || family_name == "UInt16" || family_name == "UInt32" || family_name == "UInt64")
            {
                Field f = static_cast<UInt64>(key);
                mut_col->insert(f);
            }
            else if (family_name == "String")
            {
                Field f = std::to_string(key);
                mut_col->insert(f);
            }
            else
            {
                throw std::invalid_argument(fmt::format("family name {} should not be rowkey.", family_name));
            }
            col.column = std::move(mut_col);
            block.insert(std::move(col));
        }

        // Generate 'timestamp'.
        uint64_t ts = ts_gen.get();
        {
            auto & col_def = (*table_info.columns)[1];
            if (col_def.id != VERSION_COLUMN_ID)
            {
                throw std::invalid_argument(fmt::format("(*table_info.columns)[1].id is {} not VERSION_COLUMN_ID {}.", col_def.id, VERSION_COLUMN_ID));
            }
            ColumnWithTypeAndName col({}, col_def.type, col_def.name, col_def.id);
            IColumn::MutablePtr mut_col = col.type->createColumn();
            Field f = ts;
            mut_col->insert(f);
            col.column = std::move(mut_col);
            block.insert(std::move(col));
        }

        // Generate 'delete mark'
        {
            auto & col_def = (*table_info.columns)[2];
            if (col_def.id != TAG_COLUMN_ID)
            {
                throw std::invalid_argument(fmt::format("(*table_info.columns)[2].id is {} not TAG_COLUMN_ID {}.", col_def.id, TAG_COLUMN_ID));
            }
            ColumnWithTypeAndName col({}, col_def.type, col_def.name, col_def.id);
            IColumn::MutablePtr mut_col = col.type->createColumn();
            // TODO: support random delete mark.
            Field f = static_cast<uint64_t>(0);
            mut_col->insert(f);
            col.column = std::move(mut_col);
            block.insert(std::move(col));
        }

        for (size_t i = 0; i < table_info.columns->size(); i++)
        {
            auto itr = std::find(table_info.rowkey_column_indexes.begin(), table_info.rowkey_column_indexes.end(), i);
            if (itr != table_info.rowkey_column_indexes.end())
            {
                continue;
            }
            auto & col_def = (*table_info.columns)[i];
            if (col_def.id == table_info.handle.id || col_def.id == VERSION_COLUMN_ID || col_def.id == TAG_COLUMN_ID)
            {
                continue;
            }
            auto col = createColumnWithRandomData(col_def.type, col_def.name, col_def.id);
            block.insert(std::move(col));
        }
        return {block, ts};
    }

private:
    ColumnWithTypeAndName createColumnWithRandomData(const DataTypePtr & data_type, const String & name, Int64 col_id)
    {
        ColumnWithTypeAndName col({}, data_type, name, col_id);
        IColumn::MutablePtr mut_col = col.type->createColumn();
        std::string family_name = col.type->getFamilyName();
        if (family_name == "Int8" || family_name == "Int16" || family_name == "Int32" || family_name == "Int64")
        {
            Field f = static_cast<Int64>(rand_gen());
            mut_col->insert(f);
        }
        else if (family_name == "UInt8" || family_name == "UInt16" || family_name == "UInt32" || family_name == "UInt64")
        {
            Field f = static_cast<UInt64>(rand_gen());
            mut_col->insert(f);
        }
        else if (family_name == "Float32" || family_name == "Float64")
        {
            Field f = static_cast<Float64>(real_rand_gen(rand_gen));
            mut_col->insert(f);
        }
        else if (family_name == "String")
        {
            Field f = randomString();
            mut_col->insert(f);
        }
        else if (family_name == "Enum8")
        {
            const auto * dt = dynamic_cast<const DataTypeEnum8 *>(data_type.get());
            const auto & values = dt->getValues();
            Field f = static_cast<int64_t>(values[rand_gen() % values.size()].second);
            mut_col->insert(f);
        }
        else if (family_name == "Enum16")
        {
            const auto * dt = dynamic_cast<const DataTypeEnum16 *>(data_type.get());
            const auto & values = dt->getValues();
            Field f = static_cast<int64_t>(values[rand_gen() % values.size()].second);
            mut_col->insert(f);
        }
        else if (family_name == "MyDateTime")
        {
            Field f = parseMyDateTime(randomDateTime());
            mut_col->insert(f);
        }
        else if (family_name == "MyDate")
        {
            Field f = parseMyDateTime(randomDate());
            mut_col->insert(f);
        }
        else if (family_name == "Decimal")
        {
            auto prec = getDecimalPrecision(*data_type, 0);
            auto scale = getDecimalScale(*data_type, 0);
            auto s = randomDecimal(prec, scale);
            bool negative = rand_gen() % 2 == 0;
            Field f;
            if (parseDecimal(s.data(), s.size(), negative, f))
            {
                mut_col->insert(f);
            }
            else
            {
                throw std::invalid_argument(fmt::format("RandomDataGenerator parseDecimal({}, {}) prec {} scale {} fail", s, negative, prec, scale));
            }
        }
        col.column = std::move(mut_col);
        return col;
    }

    std::string randomDecimal(uint64_t prec, uint64_t scale)
    {
        auto s = std::to_string(rand_gen());
        if (s.size() < prec)
        {
            s += std::string(prec - s.size(), '0');
        }
        else if (s.size() > prec)
        {
            s = s.substr(0, prec);
        }
        return s.substr(0, prec - scale) + "." + s.substr(prec - scale);
    }

    std::string randomDate()
    {
        auto res = randomLocalTime();
        return fmt::format("{}-{}-{}", res.tm_year + 1900, res.tm_mon + 1, res.tm_mday);
    }

    std::string randomDateTime()
    {
        auto res = randomLocalTime();
        return fmt::format("{}-{}-{} {}:{}:{}", res.tm_year + 1900, res.tm_mon + 1, res.tm_mday, res.tm_hour, res.tm_min, res.tm_sec);
    }

    time_t randomUTCTimestamp()
    {
        return ::time(nullptr) + randomTimeOffset();
    }

    int randomTimeOffset()
    {
        static constexpr int max_offset = 24 * 3600 * 10000; // 10000 days for test
        return (rand_gen() % max_offset) * (rand_gen() % 2 == 0 ? 1 : -1);
    }

    struct tm randomLocalTime()
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

    std::string randomString()
    {
        constexpr int size = 128;
        std::string str(size, 0);
        std::generate_n(str.begin(), str.size(), [this]() { return charset[rand_gen() % charset.size()]; });
        return str;
    }

    const TableInfo & table_info;
    TimestampGenerator & ts_gen;
    std::mt19937_64 rand_gen;
    std::uniform_real_distribution<double> real_rand_gen;
    const std::string charset{"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"};
};

std::unique_ptr<DataGenerator> DataGenerator::create([[maybe_unused]] const WorkloadOptions & opts, const TableInfo & table_info, TimestampGenerator & ts_gen)
{
    return std::make_unique<RandomDataGenerator>(table_info, ts_gen);
}

} // namespace DB::DM::tests
