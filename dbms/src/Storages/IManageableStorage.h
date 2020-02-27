#pragma once

#include <DataStreams/CoprocessorBlockInputStream.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/Transaction/StorageEngineType.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiKVKeyValue.h>
#include <pingcap/coprocessor/Client.h>
#include <tipb/select.pb.h>

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

    virtual void checkStatus(const Context &) {}

    virtual void deleteRows(const Context &, size_t /*rows*/) { throw Exception("Unsupported"); }

    virtual ::TiDB::StorageEngine engineType() const = 0;

    virtual String getDatabaseName() const = 0;

    virtual void setTableInfo(const TiDB::TableInfo & table_info_) = 0;

    virtual const TiDB::TableInfo & getTableInfo() const = 0;

    virtual BlockInputStreams remote_read(const std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> & key_ranges,
        const SelectQueryInfo & query_info, const tipb::TableScan & ts, Context & context)
    {
        std::vector<pingcap::coprocessor::KeyRange> cop_key_ranges;
        for (const auto & key_range : key_ranges)
        {
            cop_key_ranges.push_back(
                pingcap::coprocessor::KeyRange{static_cast<String>(key_range.first), static_cast<String>(key_range.second)});
        }

        ::tipb::DAGRequest dag_req;

        auto * exec = dag_req.add_executors();
        exec->set_tp(tipb::ExecType::TypeTableScan);
        exec->set_allocated_tbl_scan(new tipb::TableScan(ts));
        dag_req.set_encode_type(tipb::EncodeType::TypeChunk);

        DAGSchema schema;
        for (int i = 0; i < ts.columns_size(); i++)
        {
            dag_req.add_output_offsets(i);
            auto id = ts.columns(i).column_id();
            const ColumnInfo & info = getTableInfo().getColumnInfoByID(id);
            schema.push_back(std::make_pair(info.name, info));
        }

        pingcap::coprocessor::Request req;

        dag_req.SerializeToString(&req.data);
        req.tp = pingcap::coprocessor::ReqType::DAG;
        req.start_ts = query_info.mvcc_query_info->read_tso;
        req.ranges = cop_key_ranges;

        BlockInputStreamPtr input = std::make_shared<CoprocessorBlockInputStream>(context.getTMTContext().getKVCluster(), req, schema);
        return {input};
    };

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