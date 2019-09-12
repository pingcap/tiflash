#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/Transaction/StorageEngineType.h>

namespace TiDB
{
struct TableInfo;
}

namespace DB
{
/**
 * An interface for Storages synced from TiDB.
 *
 * Note that you must override `startup` and `shutdown` to register/remove this table into TMTContext.
 */
class IManageableStorage : public IStorage
{
public:
    enum class PKType
    {
        INT64 = 0,
        UINT64,
        UNSPECIFIED,
    };

public:
    explicit IManageableStorage() : IStorage() {}
    explicit IManageableStorage(const ColumnsDescription & columns_) : IStorage(columns_) {}
    ~IManageableStorage() override = default;

    virtual void flushDelta() {}

    virtual BlockInputStreamPtr status() { return {}; }

    virtual void check(const Context &) {}

    virtual ::TiDB::StorageEngine engineType() const = 0;

    virtual String getDatabaseName() const = 0;

    virtual void setTableInfo(const TiDB::TableInfo & table_info_) = 0;

    virtual const TiDB::TableInfo & getTableInfo() const = 0;

    // Apply AlterCommands synced from TiDB should use `alterFromTiDB` instead of `alter(...)`
    virtual void alterFromTiDB(
            const AlterCommands & commands, const TiDB::TableInfo & table_info, const String & database_name, const Context & context)
    = 0;

    PKType getPKType() const
    {
        static const DataTypeInt64 & dataTypeInt64 = {};
        static const DataTypeUInt64 & dataTypeUInt64 = {};

        auto pk_data_type = getPKTypeImpl();
        if (pk_data_type->equals(dataTypeInt64))
            return PKType::INT64;
        else if (pk_data_type->equals(dataTypeUInt64))
            return PKType::UINT64;
        return PKType::UNSPECIFIED;
    }

    virtual SortDescription getPrimarySortDescription() const = 0;

private:
    virtual DataTypePtr getPKTypeImpl() const = 0;
};

} // namespace DB