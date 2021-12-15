#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/DeltaMerge/tests/dm_basic_include.h>
#include <Storages/DeltaMerge/tests/workload/Options.h>
#include <Storages/DeltaMerge/tests/workload/TableGenerator.h>
#include <fmt/ranges.h>

#include <random>

namespace DB::DM::tests
{
std::vector<std::string> TableInfo::toStrings() const
{
    std::vector<std::string> v;
    v.push_back(fmt::format("db_name {} table_name {} columns count {} is_common_handle {} rowkey_column_indexes {}", db_name, table_name, columns->size(), is_common_handle, rowkey_column_indexes));
    for (size_t i = 0; i < columns->size(); i++)
    {
        auto col = (*columns)[i];
        if (col.type->getFamilyName() == std::string("Decimal"))
        {
            v.push_back(fmt::format("columns[{}]: id {} name {} type {}", i, col.id, col.name, col.type->getName()));
        }
        else
        {
            v.push_back(fmt::format("columns[{}]: id {} name {} type {}", i, col.id, col.name, col.type->getFamilyName()));
        }
    }
    return v;
}

class TablePkType
{
public:
    static TablePkType & instance()
    {
        static TablePkType table_pk_type;
        return table_pk_type;
    }

    DMTestEnv::PkType randomPkType()
    {
        return pk_types[rand_gen() % pk_types.size()].second;
    }

    DMTestEnv::PkType getPkType(const std::string & name)
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

private:
    TablePkType()
        : rand_gen(std::random_device()())
    {}

    // CommonHandle is not supported for simplifing data generation and verification.
    // PkIsHandleInt32 is not supported for simplifing verification.
    const std::vector<std::pair<std::string, DMTestEnv::PkType>> pk_types = {
        {"tidb_rowid", DMTestEnv::PkType::HiddenTiDBRowID},
        //{"common_handle", DMTestEnv::PkType::CommonHandle},
        {"pk_is_handle64", DMTestEnv::PkType::PkIsHandleInt64},
        //{"pk_is_handle32", DMTestEnv::PkType::PkIsHandleInt32},
    };

    std::mt19937_64 rand_gen;

    TablePkType(const TablePkType &) = delete;
    TablePkType(TablePkType &&) = delete;
    TablePkType & operator=(const TablePkType &) = delete;
    TablePkType && operator=(TablePkType &&) = delete;
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
        for (T i = 0; i < enum_cnt; i++)
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

    virtual TableInfo get() override
    {
        TableInfo table_info;

        table_info.db_name = "workload";
        table_info.table_name = fmt::format("random_table_{}", rand_gen());

        auto type = getPkType();
        table_info.columns = DMTestEnv::getDefaultColumns(type);

        int cols_cnt = cols_count > 0 ? cols_count : rand_gen() % max_columns_count + 1;
        for (int i = 0; i < cols_cnt; i++)
        {
            int id = i + 3;
            auto name = fmt::format("col_{}", id);
            auto data_type = getDataType();
            table_info.columns->emplace_back(ColumnDefine(id, name, data_type));
        }
        table_info.handle = (*table_info.columns)[0];
        table_info.is_common_handle = (type == DMTestEnv::PkType::CommonHandle);
        if (type != DMTestEnv::PkType::CommonHandle)
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
    DMTestEnv::PkType getPkType()
    {
        if (pk_type.empty())
        {
            return TablePkType::instance().randomPkType();
        }
        return TablePkType::instance().getPkType(pk_type);
    }

    DataTypePtr getDataType()
    {
        return TableDataType::instance().randomDataType();
    }

    const std::string pk_type;
    const int cols_count;
    std::mt19937_64 rand_gen;

    static constexpr int max_columns_count = 64;
};

class ConstantTableGenerator : public TableGenerator
{
    virtual TableInfo get() override
    {
        TableInfo table_info;

        table_info.db_name = "workload";
        table_info.table_name = "constant";

        table_info.columns = DMTestEnv::getDefaultColumns();

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
            auto data_type = TableDataType::instance().getDataType(data_cols[i], 10 /*enum_cnt*/, 20 /*prec*/, 10 /*scale*/);
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