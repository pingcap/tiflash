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

#include <Common/nocopyable.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/DeltaMerge/workload/Options.h>
#include <Storages/DeltaMerge/workload/TableGenerator.h>
#include <fmt/ranges.h>

#include <random>

namespace DB::DM::tests
{
std::vector<std::string> TableInfo::toStrings() const
{
    std::vector<std::string> v;
    v.push_back(fmt::format(
        "db_name {} table_name {} columns count {} is_common_handle {} rowkey_column_indexes {}",
        db_name,
        table_name,
        columns->size(),
        is_common_handle,
        rowkey_column_indexes));
    for (size_t i = 0; i < columns->size(); i++)
    {
        auto col = (*columns)[i];
        if (col.type->getFamilyName() == std::string("Decimal"))
        {
            v.push_back(fmt::format("columns[{}]: id {} name {} type {}", i, col.id, col.name, col.type->getName()));
        }
        else
        {
            v.push_back(
                fmt::format("columns[{}]: id {} name {} type {}", i, col.id, col.name, col.type->getFamilyName()));
        }
    }
    return v;
}

class TablePkType
{
public:
    DISALLOW_COPY_AND_MOVE(TablePkType);

    static TablePkType & instance()
    {
        static TablePkType table_pk_type;
        return table_pk_type;
    }

    enum class PkType
    {
        // If the primary key is composed of multiple columns and non-clustered-index,
        // or users don't define the primary key, TiDB will add a hidden "_tidb_rowid" column
        // as the handle column
        HiddenTiDBRowID,
        // Common handle for clustered-index since 5.0.0
        CommonHandle,
        // If user define the primary key that is compatibility with UInt64, use that column
        // as the handle column
        PkIsHandleInt64,
        PkIsHandleInt32,
    };

    PkType randomPkType() { return pk_types[rand_gen() % pk_types.size()].second; }

    PkType getPkType(const std::string & name)
    {
        for (const auto & pa : pk_types)
        {
            if (name == pa.first)
            {
                return pa.second;
            }
        }
        throw std::invalid_argument(fmt::format("PkType {} is invalid.", name));
    }

    static constexpr const char * pk_name = "_tidb_rowid";
    static constexpr const char * PK_NAME_PK_IS_HANDLE = "id";

    static ColumnDefinesPtr getDefaultColumns(PkType pk_type = PkType::HiddenTiDBRowID)
    {
        // Return [handle, ver, del] column defines
        ColumnDefinesPtr columns = std::make_shared<ColumnDefines>();
        switch (pk_type)
        {
        case PkType::HiddenTiDBRowID:
            columns->emplace_back(getExtraHandleColumnDefine(/*is_common_handle=*/false));
            break;
        case PkType::CommonHandle:
            columns->emplace_back(getExtraHandleColumnDefine(/*is_common_handle=*/true));
            break;
        case PkType::PkIsHandleInt64:
            columns->emplace_back(ColumnDefine{2, PK_NAME_PK_IS_HANDLE, EXTRA_HANDLE_COLUMN_INT_TYPE});
            break;
        case PkType::PkIsHandleInt32:
            columns->emplace_back(ColumnDefine{2, PK_NAME_PK_IS_HANDLE, DataTypeFactory::instance().get("Int32")});
            break;
        default:
            throw Exception("Unknown pk type for test");
        }
        columns->emplace_back(getVersionColumnDefine());
        columns->emplace_back(getTagColumnDefine());
        return columns;
    }

private:
    TablePkType()
        : rand_gen(std::random_device()())
    {}

    // CommonHandle is not supported for simplifing data generation and verification.
    // PkIsHandleInt32 is not supported for simplifing verification.
    const std::vector<std::pair<std::string, PkType>> pk_types = {
        {"tidb_rowid", PkType::HiddenTiDBRowID},
        //{"common_handle", PkType::CommonHandle},
        {"pk_is_handle64", PkType::PkIsHandleInt64},
        //{"pk_is_handle32", PkType::PkIsHandleInt32},
    };

    std::mt19937_64 rand_gen;
};

class TableDataType
{
public:
    static TableDataType & instance()
    {
        static TableDataType table_data_type;
        return table_data_type;
    }

    DataTypePtr randomDataType()
    {
        auto name = data_types[rand_gen() % data_types.size()];
        return getDataType(name);
    }

    DataTypePtr getDataType(const std::string & name, int enum_cnt = 0, int prec = -1, int scale = -1)
    {
        if (name == "Decimal")
        {
            return createDecimal(prec, scale);
        }
        else if (name == "Enum8")
        {
            return createEnum<int8_t>(enum_cnt);
        }
        else if (name == "Enum16")
        {
            return createEnum<int16_t>(enum_cnt);
        }
        else
        {
            return DataTypeFactory::instance().get(name);
        }
        throw std::invalid_argument(fmt::format("getDataType {} is invalid.", name));
    }

private:
    TableDataType()
        : rand_gen(std::random_device()())
    {}

    DataTypePtr createDecimal(int prec, int scale)
    {
        // Limit the max precision of decimal type to facilitate random generate data.
        static const int max_precision = std::to_string(std::numeric_limits<uint64_t>::max()).size();
        if (prec <= 0)
        {
            // Precision should not be zero.
            prec = rand_gen() % max_precision + 1;
        }
        if (scale <= -1)
        {
            // Scale is less than or equal to precision.
            scale = rand_gen() % prec;
        }
        return DB::createDecimal(prec, scale);
    }

    template <typename T>
    DataTypePtr createEnum(int enum_cnt)
    {
        // Limit the max enum value to 1024 for test.
        T max_value = std::min(static_cast<int>(std::numeric_limits<T>::max()), 1024);
        // Enum count should not be zero.
        if (enum_cnt == 0)
        {
            enum_cnt = rand_gen() % max_value + 1;
        }
        typename DataTypeEnum<T>::Values values;
        for (T i = 0; i < static_cast<T>(enum_cnt); i++)
        {
            values.emplace_back(fmt::format("e_{}", i), i);
        }
        return std::make_shared<DataTypeEnum<T>>(values);
    }

    std::mt19937_64 rand_gen;

    const std::vector<std::string> data_types = {
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "Float32",
        "Float64",
        "String",
        "MyDate",
        "MyDateTime",
        "Enum16",
        "Enum8",
        "Decimal",
    };
};

class RandomTableGenerator : public TableGenerator
{
public:
    RandomTableGenerator(const std::string & pk_type_, int cols_count_)
        : pk_type(pk_type_)
        , cols_count(cols_count_)
        , rand_gen(std::random_device()())
    {}

    TableInfo get(int64_t table_id, std::string table_name) override
    {
        TableInfo table_info;

        table_info.table_id = table_id < 0 ? rand_gen() : table_id;
        table_info.table_name = table_name.empty() ? fmt::format("t_{}", table_info.table_id) : table_name;
        table_info.db_name = "workload";

        auto type = getPkType();
        table_info.columns = TablePkType::getDefaultColumns(type);

        int cols_cnt = cols_count > 0 ? cols_count : rand_gen() % max_columns_count + 1;
        for (int i = 0; i < cols_cnt; i++)
        {
            int id = i + 3;
            auto name = fmt::format("col_{}", id);
            auto data_type = RandomTableGenerator::getDataType();
            table_info.columns->emplace_back(ColumnDefine(id, name, data_type));
        }
        table_info.handle = (*table_info.columns)[0];
        table_info.is_common_handle = (type == TablePkType::PkType::CommonHandle);
        if (type != TablePkType::PkType::CommonHandle)
        {
            table_info.rowkey_column_indexes.push_back(0);
        }
        else
        {
            throw std::invalid_argument("TableGenerator::get CommonHanle not support.");
        }
        return table_info;
    }

private:
    TablePkType::PkType getPkType()
    {
        if (pk_type.empty())
        {
            return TablePkType::instance().randomPkType();
        }
        return TablePkType::instance().getPkType(pk_type);
    }

    static DataTypePtr getDataType() { return TableDataType::instance().randomDataType(); }

    const std::string pk_type;
    const int cols_count;
    std::mt19937_64 rand_gen;

    static constexpr int max_columns_count = 64;
};

class ConstantTableGenerator : public TableGenerator
{
    TableInfo get(int64_t table_id, std::string table_name) override
    {
        TableInfo table_info;

        table_info.table_id = table_id < 0 ? 0 : table_id;
        table_info.table_name = table_name.empty() ? "constant" : table_name;
        table_info.db_name = "workload";

        table_info.columns = TablePkType::getDefaultColumns();

        const std::vector<std::string> data_cols = {
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "Float32",
            "Float64",
            "String",
            "MyDate",
            "MyDateTime",
            "Enum16",
            "Enum8",
            "Decimal",
        };
        for (size_t i = 0; i < data_cols.size(); i++)
        {
            int id = i + 3;
            auto name = fmt::format("col_{}", id);
            auto data_type
                = TableDataType::instance().getDataType(data_cols[i], 10 /*enum_cnt*/, 20 /*prec*/, 10 /*scale*/);
            table_info.columns->emplace_back(ColumnDefine(id, name, data_type));
        }
        table_info.handle = (*table_info.columns)[0];
        table_info.is_common_handle = false;
        table_info.rowkey_column_indexes.push_back(0);
        return table_info;
    }
};
std::unique_ptr<TableGenerator> TableGenerator::create(const WorkloadOptions & opts)
{
    const auto & table = opts.table;
    if (table == "constant")
    {
        return std::make_unique<ConstantTableGenerator>();
    }
    if (table == "random")
    {
        return std::make_unique<RandomTableGenerator>(opts.pk_type, opts.columns_count);
    }
    else
    {
        throw std::invalid_argument(fmt::format("TableGenerator::create '{}' not support.", table));
    }
}
} // namespace DB::DM::tests