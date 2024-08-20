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

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/FmtUtils.h>
#include <Common/Logger.h>
#include <Common/TiFlashMetrics.h>
#include <Common/formatReadable.h>
#include <Common/typeid_cast.h>
#include <Core/Defines.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/isSupportedDataTypeCast.h>
#include <Databases/IDatabase.h>
#include <Debug/MockTiDB.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/AlterCommands.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSchema.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/Filter/PushDownFilter.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/FilterParser/FilterParser.h>
#include <Storages/DeltaMerge/Remote/DisaggSnapshot.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>
#include <Storages/MutableSupport.h>
#include <Storages/PathPool.h>
#include <Storages/PrimaryKeyNotMatchException.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeHelpers.h>
#include <TiDB/Decode/TypeMapping.h>
#include <TiDB/Schema/SchemaNameMapper.h>
#include <TiDB/Schema/TiDB.h>
#include <common/logger_useful.h>


namespace DB
{
namespace FailPoints
{
extern const char exception_during_write_to_storage[];
} // namespace FailPoints

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int DIRECTORY_ALREADY_EXISTS;
} // namespace ErrorCodes

using namespace DM;

StorageDeltaMerge::StorageDeltaMerge(
    const String & db_engine,
    const String & db_name_,
    const String & table_name_,
    const OptionTableInfoConstRef table_info_,
    const ColumnsDescription & columns_,
    const ASTPtr & primary_expr_ast_,
    Timestamp tombstone,
    Context & global_context_)
    : IManageableStorage{columns_, tombstone}
    , data_path_contains_database_name(db_engine != "TiFlash")
    , store_inited(false)
    , max_column_id_used(0)
    , global_context(global_context_.getGlobalContext())
    , log(Logger::get(fmt::format("{}.{}", db_name_, table_name_)))
{
    if (primary_expr_ast_->children.empty())
        throw Exception("No primary key");

    // save schema from TiDB
    if (table_info_)
    {
        tidb_table_info = table_info_->get();
        is_common_handle = tidb_table_info.is_common_handle;
        pk_is_handle = tidb_table_info.pk_is_handle;
    }
    else
    {
        const auto mock_table_id = MockTiDB::instance().newTableID();
        tidb_table_info.id = mock_table_id;
        LOG_WARNING(log, "Allocate table id for mock test [id={}]", mock_table_id);
    }

    table_column_info = std::make_unique<TableColumnInfo>(db_name_, table_name_, primary_expr_ast_);

    updateTableColumnInfo();
}

void StorageDeltaMerge::updateTableColumnInfo()
{
    const ColumnsDescription & columns = getColumns();

    LOG_INFO(
        log,
        "updateTableColumnInfo, table_name={} ordinary=\"{}\" materialized=\"{}\"",
        table_column_info->table_name,
        columns.ordinary.toString(),
        columns.materialized.toString());

    auto & pk_expr_ast = table_column_info->pk_expr_ast;
    auto & handle_column_define = table_column_info->handle_column_define;
    auto & table_column_defines = table_column_info->table_column_defines;
    handle_column_define.name.clear();
    table_column_defines.clear();
    pk_column_names.clear();

    std::unordered_set<String> pks;
    if (!tidb_table_info.columns.empty())
    {
        if (pk_is_handle)
        {
            for (const auto & col : tidb_table_info.columns)
            {
                if (col.hasPriKeyFlag())
                {
                    pks.emplace(col.name);
                    pk_column_names.emplace_back(col.name);
                }
            }
        }
        else
        {
            pks.emplace(EXTRA_HANDLE_COLUMN_NAME);
            pk_column_names.emplace_back(EXTRA_HANDLE_COLUMN_NAME);
        }
    }
    else
    {
        for (size_t i = 0; i < pk_expr_ast->children.size(); ++i)
        {
            auto col_name = pk_expr_ast->children[i]->getColumnName();
            pks.emplace(col_name);
            pk_column_names.emplace_back(col_name);
        }
    }

    // TODO(hyy):seems aliases and default in ColumnsDescription is useless，please check if we can remove it
    ColumnsDescription new_columns(columns.ordinary, columns.materialized, columns.aliases, columns.defaults);
    size_t pks_combined_bytes = 0;
    auto all_columns = columns.getAllPhysical();

    /// rowkey_column_defines is the columns used to generate rowkey in TiDB
    /// if is_common_handle = true || pk_is_handle = true, it is the primary keys in TiDB table's definition
    /// otherwise, it is _tidb_rowid
    ColumnDefines rowkey_column_defines;
    for (const auto & col : all_columns)
    {
        ColumnDefine col_def(0, col.name, col.type);
        if (!tidb_table_info.columns.empty())
        {
            /// If TableInfo from TiDB is not empty, we get column id and default value from TiDB
            auto & columns = tidb_table_info.columns;
            col_def.id = tidb_table_info.getColumnID(col_def.name);
            auto itr = std::find_if(columns.begin(), columns.end(), [&](const ColumnInfo & v) {
                return v.id == col_def.id;
            });
            if (itr != columns.end())
            {
                col_def.default_value = itr->defaultValueToField();
            }

            if (col_def.id != TiDBPkColumnID && col_def.id != VersionColumnID && col_def.id != DelMarkColumnID
                && tidb_table_info.getColumnInfo(col_def.id).hasPriKeyFlag())
            {
                rowkey_column_defines.push_back(col_def);
            }
        }
        else
        {
            // in test cases, we allocate column_id here
            col_def.id = max_column_id_used++;
        }

        if (pks.count(col.name))
        {
            if (!is_common_handle)
            {
                if (!col.type->isValueRepresentedByInteger())
                {
                    throw Exception(fmt::format("pk column {} is not representable by integer", col.name));
                }
                pks_combined_bytes += col.type->getSizeOfValueInMemory();
                if (pks_combined_bytes > sizeof(Handle))
                {
                    throw Exception(fmt::format(
                        "pk columns bytes exceeds size limit, {} > {}",
                        pks_combined_bytes,
                        sizeof(Handle)));
                }
            }
            if (pks.size() == 1)
            {
                handle_column_define = col_def;
            }
        }
        table_column_defines.push_back(col_def);
    }

    if (!new_columns.materialized.contains(VERSION_COLUMN_NAME))
    {
        hidden_columns.emplace_back(VERSION_COLUMN_NAME);
        new_columns.materialized.emplace_back(VERSION_COLUMN_NAME, VERSION_COLUMN_TYPE);
    }
    if (!new_columns.materialized.contains(TAG_COLUMN_NAME))
    {
        hidden_columns.emplace_back(TAG_COLUMN_NAME);
        new_columns.materialized.emplace_back(TAG_COLUMN_NAME, TAG_COLUMN_TYPE);
    }

    if (pks.size() > 1)
    {
        if (unlikely(is_common_handle))
        {
            throw Exception("Should not reach here: common handle with pk size > 1", ErrorCodes::LOGICAL_ERROR);
        }
        handle_column_define.id = EXTRA_HANDLE_COLUMN_ID;
        handle_column_define.name = EXTRA_HANDLE_COLUMN_NAME;
        handle_column_define.type = EXTRA_HANDLE_COLUMN_INT_TYPE;
        if (!new_columns.materialized.contains(EXTRA_HANDLE_COLUMN_NAME))
        {
            hidden_columns.emplace_back(EXTRA_HANDLE_COLUMN_NAME);
            new_columns.materialized.emplace_back(EXTRA_HANDLE_COLUMN_NAME, EXTRA_HANDLE_COLUMN_INT_TYPE);
        }
    }

    setColumns(new_columns);

    // TODO:Could we remove this branch?
    if (unlikely(handle_column_define.name.empty()))
    {
        // If users deploy a cluster with TiFlash node with version v4.0.0~v4.0.3, and rename primary key column. They will
        // run into here.
        // For v4.0.x, there is only one column that could be the primary key ("_tidb_rowid" or int64-like column) in TiFlash.
        // It is safe for us to take the primary key column name from TiDB table info to correct the primary key of the
        // create table statement.
        // Here we throw a PrimaryKeyNotMatchException, caller (`DatabaseLoading::loadTable`) is responsible for correcting
        // the statement and retry.
        if (pks.size() == 1 && !tidb_table_info.columns.empty() && !is_common_handle)
        {
            Strings actual_pri_keys;
            for (const auto & col : tidb_table_info.columns)
            {
                if (col.hasPriKeyFlag())
                {
                    actual_pri_keys.emplace_back(col.name);
                }
            }
            if (actual_pri_keys.size() == 1)
            {
                throw PrimaryKeyNotMatchException(*pks.begin(), actual_pri_keys[0]);
            }
            // fallthrough
        }

        // Unknown bug, throw an exception.
        FmtBuffer fmt_buf;
        fmt_buf.joinStr(
            all_columns.begin(),
            all_columns.end(),
            [](const auto & col, FmtBuffer & fb) { fb.append(col.name); },
            ",");
        throw Exception(fmt::format(
            "Can not create table without primary key. Primary keys should be: {}, but only these columns are found:{}",
            fmt::join(pks, ","),
            fmt_buf.toString()));
    }
    assert(!table_column_defines.empty());

    if (!(is_common_handle || pk_is_handle))
    {
        rowkey_column_defines.clear();
        rowkey_column_defines.push_back(handle_column_define);
    }
    rowkey_column_size = rowkey_column_defines.size();
}

void StorageDeltaMerge::clearData()
{
    shutdown();
    // init the store so it can clear data
    auto & store = getAndMaybeInitStore();
    store->clearData();
}

void StorageDeltaMerge::drop()
{
    shutdown();
    // init the store so it can do the drop work
    auto & store = getAndMaybeInitStore();
    store->drop();
}

Block StorageDeltaMerge::buildInsertBlock(bool is_import, bool is_delete, const Block & old_block)
{
    Block to_write = old_block;

    if (!is_import)
    {
        // Remove the default columns generated by InterpreterInsertQuery
        if (to_write.has(EXTRA_HANDLE_COLUMN_NAME))
            to_write.erase(EXTRA_HANDLE_COLUMN_NAME);
        if (to_write.has(VERSION_COLUMN_NAME))
            to_write.erase(VERSION_COLUMN_NAME);
        if (to_write.has(TAG_COLUMN_NAME))
            to_write.erase(TAG_COLUMN_NAME);
    }

    auto & store = getAndMaybeInitStore();
    const size_t rows = to_write.rows();
    if (!to_write.has(store->getHandle().name))
    {
        // put handle column.
        auto handle_column = store->getHandle().type->createColumn();
        auto & handle_data = typeid_cast<ColumnVector<Handle> &>(*handle_column).getData();
        handle_data.resize(rows);

        size_t pk_column_count = pk_column_names.size();
        Columns pk_columns;
        std::vector<DataTypePtr> pk_column_types;
        for (auto & n : pk_column_names)
        {
            auto & col = to_write.getByName(n);
            pk_columns.push_back(col.column);
            pk_column_types.push_back(col.type);
        }

        for (size_t c = 0; c < pk_column_count; ++c)
        {
            appendIntoHandleColumn(handle_data, pk_column_types[c], pk_columns[c]);
        }

        addColumnToBlock(
            to_write,
            EXTRA_HANDLE_COLUMN_ID,
            EXTRA_HANDLE_COLUMN_NAME,
            is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE,
            std::move(handle_column));
    }

    auto block = DeltaMergeStore::addExtraColumnIfNeed(global_context, store->getHandle(), std::move(to_write));

    // add version column
    if (!block.has(VERSION_COLUMN_NAME))
    {
        auto column = VERSION_COLUMN_TYPE->createColumn();
        auto & column_data = typeid_cast<ColumnVector<UInt64> &>(*column).getData();
        column_data.resize(rows);
        for (size_t i = 0; i < rows; ++i)
        {
            column_data[i] = next_version++;
        }

        addColumnToBlock(block, VERSION_COLUMN_ID, VERSION_COLUMN_NAME, VERSION_COLUMN_TYPE, std::move(column));
    }

    // add tag column (upsert / delete)
    if (!block.has(TAG_COLUMN_NAME))
    {
        auto column = TAG_COLUMN_TYPE->createColumn();
        auto & column_data = typeid_cast<ColumnVector<UInt8> &>(*column).getData();
        column_data.resize(rows);
        UInt8 tag = is_delete ? 1 : 0;
        for (size_t i = 0; i < rows; ++i)
        {
            column_data[i] = tag;
        }

        addColumnToBlock(block, TAG_COLUMN_ID, TAG_COLUMN_NAME, TAG_COLUMN_TYPE, std::move(column));
    }

    // Set the real column id.
    auto header = store->getHeader();
    for (auto & col : block)
    {
        if (col.name == EXTRA_HANDLE_COLUMN_NAME)
            col.column_id = EXTRA_HANDLE_COLUMN_ID;
        else if (col.name == VERSION_COLUMN_NAME)
            col.column_id = VERSION_COLUMN_ID;
        else if (col.name == TAG_COLUMN_NAME)
            col.column_id = TAG_COLUMN_ID;
        else
            col.column_id = header->getByName(col.name).column_id;
    }

    return block;
}

using BlockDecorator = std::function<Block(const Block &)>;
class DMBlockOutputStream : public IBlockOutputStream
{
public:
    DMBlockOutputStream(
        const DeltaMergeStorePtr & store_,
        const BlockDecorator & decorator_,
        const Context & db_context_,
        const Settings & db_settings_)
        : store(store_)
        , header(store->getHeader())
        , decorator(decorator_)
        , db_context(db_context_)
        , db_settings(db_settings_)
    {}

    Block getHeader() const override { return *header; }

    void write(const Block & block) override
    try
    {
        // When dt_insert_max_rows (Max rows of insert blocks when write into DeltaTree Engine, default = 0) is specified,
        // the insert block will be splited into multiples.
        // Currently dt_insert_max_rows is only used for performance tests.

        if (db_settings.dt_insert_max_rows == 0)
        {
            Block to_write = decorator(block);
            store->write(db_context, db_settings, to_write);
            return;
        }

        Block new_block = decorator(block);
        auto rows = new_block.rows();
        size_t step = db_settings.dt_insert_max_rows;

        for (size_t offset = 0; offset < rows; offset += step)
        {
            size_t limit = std::min(offset + step, rows) - offset;
            Block write_block;
            for (auto & column : new_block)
            {
                auto col = column.type->createColumn();
                col->insertRangeFrom(*column.column, offset, limit);
                write_block.insert(ColumnWithTypeAndName(std::move(col), column.type, column.name, column.column_id));
            }

            store->write(db_context, db_settings, write_block);
        }
    }
    catch (DB::Exception & e)
    {
        e.addMessage(fmt::format("(while writing to table `{}`)", store->getIdent()));
        throw;
    }

private:
    DeltaMergeStorePtr store;
    BlockPtr header;
    BlockDecorator decorator;
    const Context & db_context;
    const Settings & db_settings;
};

BlockOutputStreamPtr StorageDeltaMerge::write(const ASTPtr & query, const Settings & settings)
{
    const auto & insert_query = typeid_cast<const ASTInsertQuery &>(*query);
    auto decorator = [&](const Block & block) { //
        return this->buildInsertBlock(insert_query.is_import, insert_query.is_delete, block);
    };
    return std::make_shared<DMBlockOutputStream>(getAndMaybeInitStore(), decorator, global_context, settings);
}

WriteResult StorageDeltaMerge::write(
    Block & block,
    const Settings & settings,
    const RegionAppliedStatus & applied_status)
{
    auto & store = getAndMaybeInitStore();
#ifndef NDEBUG
    {
        // Do some check under DEBUG mode to ensure all block are written with column id properly set.
        // In this way we can catch the case that upstream raft log contains problematic data written from TiDB.
        auto header = store->getHeader();
        bool ok = true;
        String name;
        ColumnID cid = 0;
        for (auto & col : block)
        {
            name = col.name;
            cid = col.column_id;
            if (col.name == EXTRA_HANDLE_COLUMN_NAME)
            {
                if (col.column_id != EXTRA_HANDLE_COLUMN_ID)
                {
                    ok = false;
                    break;
                }
            }
            else if (col.name == VERSION_COLUMN_NAME)
            {
                if (col.column_id != VERSION_COLUMN_ID)
                {
                    ok = false;
                    break;
                }
            }
            else if (col.name == TAG_COLUMN_NAME)
            {
                if (col.column_id != TAG_COLUMN_ID)
                {
                    ok = false;
                    break;
                }
            }
            // it's ok if some columns in block is not in storage header, because these columns should be dropped after generating the block
            else if (header->has(col.name))
            {
                auto & header_col = header->getByName(col.name);
                if (col.column_id != header_col.column_id)
                {
                    ok = false;
                    break;
                }
                // We don't need to set default_value by now
                // col.default_value = header_col.default_value;
            }
        }
        if (!ok)
        {
            throw Exception(
                fmt::format("The column-id in written block is not properly set [name={}] [id={}]", name, cid));
        }
    }
#endif

    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_write_to_storage);

    return store->write(global_context, settings, block, applied_status);
}

namespace
{
std::unordered_set<UInt64> parseSegmentSet(const ASTPtr & ast)
{
    if (!ast)
        return {};
    const auto & partition_ast = typeid_cast<const ASTPartition &>(*ast);

    if (!partition_ast.value)
        return {parse<UInt64>(partition_ast.id)};

    auto parse_segment_id = [](const ASTLiteral * literal) -> std::pair<bool, UInt64> {
        if (!literal)
            return {false, 0};
        switch (literal->value.getType())
        {
        case Field::Types::String:
            return {true, parse<UInt64>(literal->value.get<String>())};
        case Field::Types::UInt64:
            return {true, literal->value.get<UInt64>()};
        default:
            return {false, 0};
        }
    };

    {
        const auto * partition_lit = typeid_cast<const ASTLiteral *>(partition_ast.value.get());
        auto [suc, id] = parse_segment_id(partition_lit);
        if (suc)
            return {id};
    }

    const auto * partition_function = typeid_cast<const ASTFunction *>(partition_ast.value.get());
    if (partition_function && partition_function->name == "tuple")
    {
        std::unordered_set<UInt64> ids;
        bool ok = true;
        for (const auto & item : partition_function->arguments->children)
        {
            const auto * partition_lit = typeid_cast<const ASTLiteral *>(item.get());
            auto [suc, id] = parse_segment_id(partition_lit);
            if (suc)
            {
                ids.emplace(id);
            }
            else
            {
                ok = false;
                break;
            }
        }
        if (ok)
            return ids;
    }

    throw Exception(
        fmt::format("Unable to parse segment IDs in literal form: `{}`", partition_ast.fields_str.toString()));
}

void setColumnsToRead(
    const DeltaMergeStorePtr & store,
    ColumnDefines & columns_to_read,
    size_t & extra_table_id_index,
    const Names & column_names)
{
    auto header = store->getHeader();
    for (size_t i = 0; i < column_names.size(); i++)
    {
        ColumnDefine col_define;
        if (column_names[i] == EXTRA_HANDLE_COLUMN_NAME)
            col_define = store->getHandle();
        else if (column_names[i] == VERSION_COLUMN_NAME)
            col_define = getVersionColumnDefine();
        else if (column_names[i] == TAG_COLUMN_NAME)
            col_define = getTagColumnDefine();
        else if (column_names[i] == EXTRA_TABLE_ID_COLUMN_NAME)
        {
            extra_table_id_index = i;
            continue;
        }
        else
        {
            auto & column = header->getByName(column_names[i]);
            col_define.name = column.name;
            col_define.id = column.column_id;
            col_define.type = column.type;
            col_define.default_value = column.default_value;
        }
        columns_to_read.push_back(col_define);
    }
}

// Check whether tso is smaller than TiDB GcSafePoint
void checkStartTs(UInt64 start_ts, const Context & context, const String & req_id, KeyspaceID keyspace_id)
{
    auto & tmt = context.getTMTContext();
    RUNTIME_CHECK(tmt.isInitialized());
    auto pd_client = tmt.getPDClient();
    if (unlikely(pd_client->isMock()))
        return;
    auto safe_point = PDClientHelper::getGCSafePointWithRetry(
        pd_client,
        keyspace_id,
        /* ignore_cache= */ false,
        context.getSettingsRef().safe_point_update_interval_seconds);
    if (start_ts < safe_point)
    {
        throw TiFlashException(
            Errors::Coprocessor::BadRequest,
            "read tso is smaller than tidb gc safe point! start_ts={} safepoint={} req={}",
            start_ts,
            safe_point,
            req_id);
    }
}

DM::RowKeyRanges parseMvccQueryInfo(
    const DB::MvccQueryInfo & mvcc_query_info,
    KeyspaceID keyspace_id,
    TableID table_id,
    bool is_common_handle,
    size_t rowkey_column_size,
    unsigned num_streams,
    const Context & context,
    const String & req_id,
    const LoggerPtr & tracing_logger)
{
    checkStartTs(mvcc_query_info.start_ts, context, req_id, keyspace_id);

    FmtBuffer fmt_buf;
    if (unlikely(tracing_logger->is(Poco::Message::Priority::PRIO_TRACE)))
    {
        fmt_buf.append("orig, ");
        fmt_buf.joinStr(
            mvcc_query_info.regions_query_info.begin(),
            mvcc_query_info.regions_query_info.end(),
            [](const auto & region, FmtBuffer & fb) {
                if (!region.required_handle_ranges.empty())
                {
                    fb.joinStr(
                        region.required_handle_ranges.begin(),
                        region.required_handle_ranges.end(),
                        [region_id = region.region_id](const auto & range, FmtBuffer & fb) {
                            fb.fmtAppend("{}{}", region_id, RecordKVFormat::DecodedTiKVKeyRangeToDebugString(range));
                        },
                        ",");
                }
                else
                {
                    /// only used for test cases
                    const auto & range = region.range_in_table;
                    fb.fmtAppend("{}{}", region.region_id, RecordKVFormat::DecodedTiKVKeyRangeToDebugString(range));
                }
            },
            ",");
    }

    auto ranges = getQueryRanges(
        mvcc_query_info.regions_query_info,
        table_id,
        is_common_handle,
        rowkey_column_size,
        num_streams,
        tracing_logger);

    if (unlikely(tracing_logger->is(Poco::Message::Priority::PRIO_TRACE)))
    {
        fmt_buf.append(" merged, ");
        fmt_buf.joinStr(
            ranges.begin(),
            ranges.end(),
            [](const auto & range, FmtBuffer & fb) { fb.append(range.toDebugString()); },
            ",");
        LOG_TRACE(tracing_logger, "reading ranges: {}", fmt_buf.toString());
    }

    return ranges;
}

RuntimeFilteList parseRuntimeFilterList(
    const SelectQueryInfo & query_info,
    const DM::ColumnDefines & table_column_defines,
    const Context & db_context,
    const LoggerPtr & log)
{
    if (db_context.getDAGContext() == nullptr || query_info.dag_query == nullptr)
    {
        return std::vector<RuntimeFilterPtr>{};
    }
    auto runtime_filter_list = db_context.getDAGContext()->runtime_filter_mgr.getLocalRuntimeFilterByIds(
        query_info.dag_query->runtime_filter_ids);
    LOG_DEBUG(log, "build runtime filter in local stream, list size:{}", runtime_filter_list.size());
    for (auto & rf : runtime_filter_list)
    {
        rf->setTargetAttr(query_info.dag_query->source_columns, table_column_defines);
    }
    return runtime_filter_list;
}
} // namespace


BlockInputStreams StorageDeltaMerge::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    auto & store = getAndMaybeInitStore();
    // Note that `columns_to_read` should keep the same sequence as ColumnRef
    // in `Coprocessor.TableScan.columns`, or rough set filter could be
    // failed to parsed.
    ColumnDefines columns_to_read;
    size_t extra_table_id_index = InvalidColumnID;
    setColumnsToRead(store, columns_to_read, extra_table_id_index, column_names);

    const ASTSelectQuery & select_query = typeid_cast<const ASTSelectQuery &>(*query_info.query);
    if (select_query.raw_for_mutable) // for selraw
    {
        // Read without MVCC filtering and del_mark = 1 filtering
        return store->readRaw(
            context,
            context.getSettingsRef(),
            columns_to_read,
            num_streams,
            query_info.keep_order,
            parseSegmentSet(select_query.segment_expression_list),
            extra_table_id_index);
    }

    auto tracing_logger = log->getChild(query_info.req_id);

    // Read with MVCC filtering
    RUNTIME_CHECK(query_info.mvcc_query_info != nullptr);
    const auto & mvcc_query_info = *query_info.mvcc_query_info;
    const auto keyspace_id = getTableInfo().getKeyspaceID();
    auto ranges = parseMvccQueryInfo(
        mvcc_query_info,
        keyspace_id,
        tidb_table_info.id,
        is_common_handle,
        rowkey_column_size,
        num_streams,
        context,
        query_info.req_id,
        tracing_logger);

    auto filter = PushDownFilter::build(query_info, columns_to_read, store->getTableColumns(), context, tracing_logger);

    auto runtime_filter_list = parseRuntimeFilterList(query_info, store->getTableColumns(), context, tracing_logger);

    const auto & scan_context = mvcc_query_info.scan_context;

    auto streams = store->read(
        context,
        context.getSettingsRef(),
        columns_to_read,
        ranges,
        num_streams,
        /*start_ts=*/mvcc_query_info.start_ts,
        filter,
        runtime_filter_list,
        query_info.dag_query == nullptr ? 0 : query_info.dag_query->rf_max_wait_time_ms,
        query_info.req_id,
        query_info.keep_order,
        /* is_fast_scan */ query_info.is_fast_scan,
        max_block_size,
        parseSegmentSet(select_query.segment_expression_list),
        extra_table_id_index,
        scan_context);

    /// Ensure start_ts info after read.
    checkStartTs(mvcc_query_info.start_ts, context, query_info.req_id, keyspace_id);

    LOG_TRACE(tracing_logger, "[ranges: {}] [streams: {}]", ranges.size(), streams.size());

    return streams;
}

void StorageDeltaMerge::read(
    PipelineExecutorContext & exec_context_,
    PipelineExecGroupBuilder & group_builder,
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    size_t max_block_size,
    unsigned num_streams)
{
    auto & store = getAndMaybeInitStore();
    // Note that `columns_to_read` should keep the same sequence as ColumnRef
    // in `Coprocessor.TableScan.columns`, or rough set filter could be
    // failed to parsed.
    ColumnDefines columns_to_read;
    size_t extra_table_id_index = InvalidColumnID;
    setColumnsToRead(store, columns_to_read, extra_table_id_index, column_names);

    const ASTSelectQuery & select_query = typeid_cast<const ASTSelectQuery &>(*query_info.query);
    if (select_query.raw_for_mutable) // for selraw
    {
        // Read without MVCC filtering and del_mark = 1 filtering
        store->readRaw(
            exec_context_,
            group_builder,
            context,
            context.getSettingsRef(),
            columns_to_read,
            num_streams,
            query_info.keep_order,
            parseSegmentSet(select_query.segment_expression_list),
            extra_table_id_index);
        return;
    }

    auto tracing_logger = log->getChild(query_info.req_id);

    // Read with MVCC filtering
    RUNTIME_CHECK(query_info.mvcc_query_info != nullptr);
    const auto & mvcc_query_info = *query_info.mvcc_query_info;
    const auto keyspace_id = getTableInfo().getKeyspaceID();
    auto ranges = parseMvccQueryInfo(
        mvcc_query_info,
        keyspace_id,
        tidb_table_info.id,
        is_common_handle,
        rowkey_column_size,
        num_streams,
        context,
        query_info.req_id,
        tracing_logger);

    auto filter = PushDownFilter::build(query_info, columns_to_read, store->getTableColumns(), context, tracing_logger);

    auto runtime_filter_list = parseRuntimeFilterList(query_info, store->getTableColumns(), context, tracing_logger);

    const auto & scan_context = mvcc_query_info.scan_context;

    store->read(
        exec_context_,
        group_builder,
        context,
        context.getSettingsRef(),
        columns_to_read,
        ranges,
        num_streams,
        /*start_ts=*/mvcc_query_info.start_ts,
        filter,
        runtime_filter_list,
        query_info.dag_query == nullptr ? 0 : query_info.dag_query->rf_max_wait_time_ms,
        query_info.req_id,
        query_info.keep_order,
        /* is_fast_scan */ query_info.is_fast_scan,
        max_block_size,
        parseSegmentSet(select_query.segment_expression_list),
        extra_table_id_index,
        scan_context);

    /// Ensure start_ts info after read.
    checkStartTs(mvcc_query_info.start_ts, context, query_info.req_id, keyspace_id);

    LOG_TRACE(tracing_logger, "[ranges: {}] [concurrency: {}]", ranges.size(), group_builder.concurrency());
}

DM::Remote::DisaggPhysicalTableReadSnapshotPtr StorageDeltaMerge::writeNodeBuildRemoteReadSnapshot(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    unsigned num_streams)
{
    auto & store = getAndMaybeInitStore();
    ColumnDefines columns_to_read;
    size_t extra_table_id_index = InvalidColumnID;
    setColumnsToRead(store, columns_to_read, extra_table_id_index, column_names);

    auto tracing_logger = log->getChild(query_info.req_id);

    const ASTSelectQuery & select_query = typeid_cast<const ASTSelectQuery &>(*query_info.query);
    RUNTIME_CHECK(query_info.mvcc_query_info != nullptr);
    const auto keyspace_id = getTableInfo().getKeyspaceID();
    const auto & mvcc_query_info = *query_info.mvcc_query_info;
    auto ranges = parseMvccQueryInfo(
        mvcc_query_info,
        keyspace_id,
        tidb_table_info.id,
        is_common_handle,
        rowkey_column_size,
        num_streams,
        context,
        query_info.req_id,
        tracing_logger);
    auto read_segments = parseSegmentSet(select_query.segment_expression_list);

    auto snap = store->writeNodeBuildRemoteReadSnapshot(
        context,
        context.getSettingsRef(),
        ranges,
        num_streams,
        query_info.req_id,
        read_segments,
        mvcc_query_info.scan_context);

    snap->column_defines = std::make_shared<ColumnDefines>(columns_to_read);

    // Ensure start_ts is valid after snapshot is built
    checkStartTs(mvcc_query_info.start_ts, context, query_info.req_id, keyspace_id);
    return snap;
}

void StorageDeltaMerge::checkStatus(const Context & context)
{
    getAndMaybeInitStore()->check(context);
}

void StorageDeltaMerge::flushCache(const Context & context)
{
    flushCache(context, DM::RowKeyRange::newAll(is_common_handle, rowkey_column_size), /* try_until_succeed */ true);
}

bool StorageDeltaMerge::flushCache(
    const Context & context,
    const DM::RowKeyRange & range_to_flush,
    bool try_until_succeed)
{
    return getAndMaybeInitStore()->flushCache(context, range_to_flush, try_until_succeed);
}

void StorageDeltaMerge::mergeDelta(const Context & context)
{
    getAndMaybeInitStore()->mergeDeltaAll(context);
}

std::optional<DM::RowKeyRange> StorageDeltaMerge::mergeDeltaBySegment(
    const Context & context,
    const DM::RowKeyValue & start_key)
{
    return getAndMaybeInitStore()->mergeDeltaBySegment(context, start_key);
}

void StorageDeltaMerge::deleteRange(const DM::RowKeyRange & range_to_delete, const Settings & settings)
{
    GET_METRIC(tiflash_storage_command_count, type_delete_range).Increment();
    return getAndMaybeInitStore()->deleteRange(global_context, settings, range_to_delete);
}

void StorageDeltaMerge::cleanPreIngestFiles(
    const std::vector<DM::ExternalDTFileInfo> & external_files,
    const Settings & settings)
{
    getAndMaybeInitStore()->cleanPreIngestFiles(global_context, settings, external_files);
}

UInt64 StorageDeltaMerge::ingestFiles(
    const DM::RowKeyRange & range,
    const std::vector<DM::ExternalDTFileInfo> & external_files,
    bool clear_data_in_range,
    const Settings & settings)
{
    GET_METRIC(tiflash_storage_command_count, type_ingest).Increment();
    return getAndMaybeInitStore()->ingestFiles(global_context, settings, range, external_files, clear_data_in_range);
}

DM::Segments StorageDeltaMerge::buildSegmentsFromCheckpointInfo(
    const DM::RowKeyRange & range,
    CheckpointInfoPtr checkpoint_info,
    const Settings & settings)
{
    return getAndMaybeInitStore()->buildSegmentsFromCheckpointInfo(global_context, settings, range, checkpoint_info);
}

UInt64 StorageDeltaMerge::ingestSegmentsFromCheckpointInfo(
    const DM::RowKeyRange & range,
    const CheckpointIngestInfoPtr & checkpoint_info,
    const Settings & settings)
{
    GET_METRIC(tiflash_storage_command_count, type_ingest_checkpoint).Increment();
    return getAndMaybeInitStore()->ingestSegmentsFromCheckpointInfo(global_context, settings, range, checkpoint_info);
}

UInt64 StorageDeltaMerge::onSyncGc(Int64 limit, const GCOptions & gc_options)
{
    if (storeInited())
    {
        return _store->onSyncGc(limit, gc_options);
    }
    return 0;
}

// just for testing
size_t getRows(DM::DeltaMergeStorePtr & store, const Context & context, const DM::RowKeyRange & range)
{
    size_t rows = 0;

    ColumnDefines to_read{getExtraHandleColumnDefine(store->isCommonHandle())};
    auto stream = store->read(
        context,
        context.getSettingsRef(),
        to_read,
        {range},
        1,
        std::numeric_limits<UInt64>::max(),
        EMPTY_FILTER,
        std::vector<RuntimeFilterPtr>(),
        0,
        /*tracing_id*/ "getRows",
        /*keep_order*/ false)[0];
    stream->readPrefix();
    Block block;
    while ((block = stream->read()))
        rows += block.rows();
    stream->readSuffix();

    return rows;
}

// just for testing
DM::RowKeyRange getRange(DM::DeltaMergeStorePtr & store, const Context & context, size_t total_rows, size_t delete_rows)
{
    auto start_index = rand() % (total_rows - delete_rows + 1); // NOLINT(cert-msc50-cpp)
    DM::RowKeyRange range = DM::RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize());
    {
        ColumnDefines to_read{getExtraHandleColumnDefine(store->isCommonHandle())};
        auto stream = store->read(
            context,
            context.getSettingsRef(),
            to_read,
            {DM::RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
            1,
            std::numeric_limits<UInt64>::max(),
            EMPTY_FILTER,
            std::vector<RuntimeFilterPtr>(),
            0,
            /*tracing_id*/ "getRange",
            /*keep_order*/ false)[0];
        stream->readPrefix();
        Block block;
        size_t index = 0;
        while ((block = stream->read()))
        {
            auto data = RowKeyColumnContainer(block.getByPosition(0).column, store->isCommonHandle());
            for (size_t i = 0; i < data.column->size(); ++i)
            {
                if (index == start_index)
                    range.setStart(data.getRowKeyValue(i).toRowKeyValue());
                if (index == start_index + delete_rows)
                    range.setEnd(data.getRowKeyValue(i).toRowKeyValue());
                ++index;
            }
        }
        stream->readSuffix();
    }

    return range;
}

void StorageDeltaMerge::deleteRows(const Context & context, size_t delete_rows)
{
    auto & store = getAndMaybeInitStore();
    size_t total_rows = getRows(store, context, DM::RowKeyRange::newAll(is_common_handle, rowkey_column_size));
    delete_rows = std::min(total_rows, delete_rows);
    auto delete_range = getRange(store, context, total_rows, delete_rows);
    size_t actual_delete_rows = getRows(store, context, delete_range);
    if (actual_delete_rows != delete_rows)
        LOG_ERROR(log, "Expected delete rows: {}, got: {}", delete_rows, actual_delete_rows);

    store->deleteRange(context, context.getSettingsRef(), delete_range);

    size_t after_delete_rows = getRows(store, context, DM::RowKeyRange::newAll(is_common_handle, rowkey_column_size));
    if (after_delete_rows != total_rows - delete_rows)
        LOG_ERROR(
            log,
            "Rows after delete range not match, expected: {}, got: {}",
            (total_rows - delete_rows),
            after_delete_rows);
}

DM::DeltaMergeStorePtr StorageDeltaMerge::getStoreIfInited() const
{
    if (storeInited())
    {
        return _store;
    }
    return nullptr;
}

std::pair<DB::DecodingStorageSchemaSnapshotConstPtr, BlockUPtr> StorageDeltaMerge::getSchemaSnapshotAndBlockForDecoding(
    const TableStructureLockHolder & table_structure_lock,
    bool need_block,
    bool with_version_column)
{
    (void)table_structure_lock;
    std::lock_guard lock{decode_schema_mutex};
    if (!decoding_schema_snapshot || decoding_schema_changed)
    {
        auto & store = getAndMaybeInitStore();
        decoding_schema_snapshot = std::make_shared<DecodingStorageSchemaSnapshot>(
            store->getStoreColumns(),
            tidb_table_info,
            store->getHandle(),
            decoding_schema_epoch++,
            with_version_column);
        cache_blocks.clear();
        decoding_schema_changed = false;
    }

    if (need_block)
    {
        if (cache_blocks.empty() || !with_version_column)
        {
            BlockUPtr block
                = std::make_unique<Block>(createBlockSortByColumnID(decoding_schema_snapshot, with_version_column));
            auto digest = hashSchema(*block);
            auto schema = global_context.getSharedBlockSchemas()->find(digest);
            if (schema)
            {
                // Because we use sha256 to calculate the hash of schema, so schemas has extremely low probability of collision
                // while we can't guarantee that there will be no collision forever,
                // so (when schema changes) we will check if this schema causes a hash collision, i.e.
                // the two different schemas have the same digest.
                // Considering there is extremely low probability for same digest but different schema,
                // we choose just throw exception when this happens.
                // If unfortunately it happens,
                // we can rename some columns in this table and then restart tiflash to workaround.
                RUNTIME_CHECK_MSG(
                    isSameSchema(*block, schema->getSchema()),
                    "new table's schema's digest is the same as one previous table schemas' digest, \
                    but schema info is not the same .So please change the new tables' schema, \
                    whose table_info is {}. The collisioned schema is {}",
                    tidb_table_info.serialize(),
                    schema->toString());
            }

            return std::make_pair(decoding_schema_snapshot, std::move(block));
        }
        else
        {
            auto block_ptr = std::move(cache_blocks.back());
            cache_blocks.pop_back();
            return std::make_pair(decoding_schema_snapshot, std::move(block_ptr));
        }
    }
    else
    {
        return std::make_pair(decoding_schema_snapshot, nullptr);
    }
}

void StorageDeltaMerge::releaseDecodingBlock(Int64 block_decoding_schema_epoch, BlockUPtr block_ptr)
{
    std::lock_guard lock{decode_schema_mutex};
    if (!decoding_schema_snapshot || block_decoding_schema_epoch < decoding_schema_snapshot->decoding_schema_epoch)
        return;
    if (cache_blocks.size() >= max_cached_blocks_num)
        return;
    clearBlockData(*block_ptr);
    cache_blocks.emplace_back(std::move(block_ptr));
}

//==========================================================================================
// DDL methods.
//==========================================================================================
void StorageDeltaMerge::updateTombstone(
    const TableLockHolder &,
    const AlterCommands & commands,
    const String & database_name,
    const TiDB::TableInfo & table_info,
    const SchemaNameMapper & name_mapper,
    const Context & context)
{
    alterImpl(commands, database_name, name_mapper.mapTableName(table_info), context);
}

void StorageDeltaMerge::alter(
    const TableLockHolder &,
    const AlterCommands & commands,
    const String & database_name,
    const String & table_name_,
    const Context & context)
{
    alterImpl(commands, database_name, table_name_, context);
}

/// If any ddl statement change StorageDeltaMerge's schema,
/// we need to update the create statement in metadata, so that we can restore table structure next time
static void updateDeltaMergeTableCreateStatement(
    const String & database_name,
    const String & table_name,
    const SortDescription & pk_names,
    const ColumnsDescription & columns,
    const OrderedNameSet & hidden_columns,
    OptionTableInfoConstRef table_info,
    Timestamp tombstone,
    const Context & context);

inline OptionTableInfoConstRef getTableInfoForCreateStatement(
    const OptionTableInfoConstRef table_info_from_tidb,
    TiDB::TableInfo & table_info_from_store,
    const ColumnDefines & store_table_columns,
    const OrderedNameSet & hidden_columns)
{
    if (likely(table_info_from_tidb))
        return table_info_from_tidb;

    /// If TableInfo from TiDB is empty, for example, create DM table for test,
    /// we refine TableInfo from store's table column, so that we can restore column id next time
    table_info_from_store.schema_version = DEFAULT_UNSPECIFIED_SCHEMA_VERSION;
    for (const auto & column_define : store_table_columns)
    {
        if (hidden_columns.has(column_define.name))
            continue;
        TiDB::ColumnInfo column_info = reverseGetColumnInfo( //
            NameAndTypePair{column_define.name, column_define.type},
            column_define.id,
            column_define.default_value,
            /* for_test= */ true);
        table_info_from_store.columns.emplace_back(std::move(column_info));
    }
    return std::optional<std::reference_wrapper<const TiDB::TableInfo>>(table_info_from_store);
}

void StorageDeltaMerge::alterImpl(
    const AlterCommands & commands,
    const String & database_name,
    const String & table_name_,
    const Context & context)
{
    auto tombstone = getTombstone();

    for (const auto & command : commands)
    {
        if (command.type == AlterCommand::TOMBSTONE)
        {
            tombstone = command.tombstone;
        }
        else if (command.type == AlterCommand::RECOVER)
        {
            tombstone = 0;
        }
    }

    updateDeltaMergeTableCreateStatement(
        database_name,
        table_name_,
        getPrimarySortDescription(),
        getColumns(),
        hidden_columns,
        getTableInfo(),
        tombstone,
        context);
    setTombstone(tombstone);
}

NamesAndTypes getColumnsFromTableInfo(const TiDB::TableInfo & table_info)
{
    NamesAndTypes columns;
    for (const auto & column : table_info.columns)
    {
        DataTypePtr type = getDataTypeByColumnInfo(column);
        columns.emplace_back(column.name, type);
    }

    if (!table_info.pk_is_handle)
    {
        // Make primary key as a column, and make the handle column as the primary key.
        if (table_info.is_common_handle)
            columns.emplace_back(MutableSupport::tidb_pk_column_name, std::make_shared<DataTypeString>());
        else
            columns.emplace_back(MutableSupport::tidb_pk_column_name, std::make_shared<DataTypeInt64>());
    }

    return columns;
}

ColumnsDescription StorageDeltaMerge::getNewColumnsDescription(const TiDB::TableInfo & table_info)
{
    auto columns = getColumnsFromTableInfo(table_info);

    ColumnsDescription new_columns;
    for (const auto & column : columns)
    {
        new_columns.ordinary.emplace_back(std::move(column));
    }

    new_columns.materialized = getColumns().materialized;

    return new_columns;
}

void StorageDeltaMerge::alterSchemaChange(
    const TableLockHolder &,
    TiDB::TableInfo & table_info,
    const String & database_name,
    const String & table_name,
    const Context & context)
{
    /// 1. update columnsDescription of ITableDeclaration
    /// 2. update table info
    /// 3. update store's columns
    /// 4. update create table statement

    ColumnsDescription new_columns = getNewColumnsDescription(table_info);

    setColumns(std::move(new_columns));

    tidb_table_info = table_info;
    LOG_DEBUG(log, "Update table_info: {} => {}", tidb_table_info.serialize(), table_info.serialize());

    {
        std::lock_guard lock(store_mutex); // Avoid concurrent init store and DDL.
        if (storeInited())
        {
            _store->applySchemaChanges(table_info);
        }
        else // it seems we will never come into this branch ?
        {
            updateTableColumnInfo();
        }
    }
    decoding_schema_changed = true;

    SortDescription pk_desc = getPrimarySortDescription();
    ColumnDefines store_columns = getStoreColumnDefines();

    // after update `new_columns` and store's table columns, we need to update create table statement,
    // so that we can restore table next time.
    updateDeltaMergeTableCreateStatement(
        database_name,
        table_name,
        pk_desc,
        getColumns(),
        hidden_columns,
        table_info,
        getTombstone(),
        context);
}

ColumnDefines StorageDeltaMerge::getStoreColumnDefines() const
{
    if (storeInited())
    {
        return _store->getTableColumns();
    }
    std::lock_guard lock(store_mutex);
    if (storeInited())
    {
        return _store->getTableColumns();
    }
    ColumnDefines cols;
    cols.emplace_back(table_column_info->handle_column_define);
    cols.emplace_back(getVersionColumnDefine());
    cols.emplace_back(getTagColumnDefine());
    for (const auto & col : table_column_info->table_column_defines)
    {
        if (col.id != table_column_info->handle_column_define.id && col.id != VERSION_COLUMN_ID
            && col.id != TAG_COLUMN_ID)
        {
            cols.emplace_back(col);
        }
    }
    return cols;
}

String StorageDeltaMerge::getName() const
{
    return MutableSupport::delta_tree_storage_name;
}

void StorageDeltaMerge::rename(
    const String & new_path_to_db,
    const String & new_database_name,
    const String & new_table_name,
    const String & new_display_table_name)
{
    tidb_table_info.name = new_display_table_name; // update name in table info
    {
        // For DatabaseTiFlash, simply update store's database is OK.
        // `store->getTableName() == new_table_name` only keep for mock test.
        bool clean_rename = !data_path_contains_database_name && getTableName() == new_table_name;
        RUNTIME_ASSERT(
            clean_rename,
            log,
            "should never rename the directories when renaming table, new_database_name={}, new_table_name={}",
            new_database_name,
            new_table_name);
    }
    if (storeInited())
    {
        _store->rename(new_path_to_db, new_database_name, new_table_name);
        return;
    }
    std::lock_guard lock(store_mutex);
    if (storeInited())
    {
        _store->rename(new_path_to_db, new_database_name, new_table_name);
    }
    else
    {
        table_column_info->db_name = new_database_name;
        table_column_info->table_name = new_table_name;
    }
}

String StorageDeltaMerge::getTableName() const
{
    if (storeInited())
    {
        return _store->getTableMeta().table_name;
    }
    std::lock_guard lock(store_mutex);
    if (storeInited())
    {
        return _store->getTableMeta().table_name;
    }
    return table_column_info->table_name;
}

String StorageDeltaMerge::getDatabaseName() const
{
    if (storeInited())
    {
        return _store->getTableMeta().db_name;
    }
    std::lock_guard lock(store_mutex);
    if (storeInited())
    {
        return _store->getTableMeta().db_name;
    }
    return table_column_info->db_name;
}

void updateDeltaMergeTableCreateStatement(
    const String & database_name,
    const String & table_name,
    const SortDescription & pk_names,
    const ColumnsDescription & columns,
    const OrderedNameSet & hidden_columns,
    OptionTableInfoConstRef table_info,
    Timestamp tombstone,
    const Context & context)
{
    /// Filter out hidden columns in the `create table statement`
    ColumnsDescription columns_without_hidden;
    columns_without_hidden.ordinary = columns.ordinary;
    for (const auto & col : columns.materialized)
        if (!hidden_columns.has(col.name))
            columns_without_hidden.materialized.emplace_back(col);
    columns_without_hidden.aliases = columns.aliases;
    columns_without_hidden.defaults = columns.defaults;

    // We need to update the JSON field in table ast
    // engine = DeltaMerge((CounterID, EventDate), '{JSON format table info}')
    IDatabase::ASTModifier storage_modifier = [&](IAST & ast) {
        ASTPtr pk_ast;
        {
            if (pk_names.size() > 1)
            {
                pk_ast = makeASTFunction("tuple");
                for (const auto & pk : pk_names)
                {
                    pk_ast->children.emplace_back(std::make_shared<ASTIdentifier>(pk.column_name));
                }
            }
            else if (pk_names.size() == 1)
            {
                pk_ast = std::make_shared<ASTExpressionList>();
                pk_ast->children.emplace_back(std::make_shared<ASTIdentifier>(pk_names[0].column_name));
            }
            else
            {
                throw Exception(fmt::format(
                    "Try to update table({}.{}) statement with no primary key. ",
                    database_name,
                    table_name));
            }
        }

        std::shared_ptr<ASTLiteral> tableinfo_literal
            = std::make_shared<ASTLiteral>(Field(table_info->get().serialize()));
        auto tombstone_ast = std::make_shared<ASTLiteral>(Field(tombstone));

        auto & storage_ast = typeid_cast<ASTStorage &>(ast);
        auto & args = typeid_cast<ASTExpressionList &>(*storage_ast.engine->arguments);
        if (!args.children.empty())
        {
            // Refresh primary keys' name
            args.children[0] = pk_ast;
        }
        if (args.children.size() == 1)
        {
            args.children.emplace_back(tableinfo_literal);
            args.children.emplace_back(tombstone_ast);
        }
        else if (args.children.size() == 2)
        {
            args.children.back() = tableinfo_literal;
            args.children.emplace_back(tombstone_ast);
        }
        else if (args.children.size() == 3)
        {
            args.children.at(1) = tableinfo_literal;
            args.children.back() = tombstone_ast;
        }
        else
        {
            throw Exception(
                fmt::format(
                    "Wrong arguments num: {} in table: {} with engine={}",
                    args.children.size(),
                    table_name,
                    MutableSupport::delta_tree_storage_name),
                ErrorCodes::BAD_ARGUMENTS);
        }
    };

    context.getDatabase(database_name)->alterTable(context, table_name, columns_without_hidden, storage_modifier);
}

// somehow duplicated with `storage_modifier` in updateDeltaMergeTableCreateStatement ...
void StorageDeltaMerge::modifyASTStorage(ASTStorage * storage_ast, const TiDB::TableInfo & table_info_)
{
    if (!storage_ast || !storage_ast->engine)
        return;
    auto * args = typeid_cast<ASTExpressionList *>(storage_ast->engine->arguments.get());
    if (!args)
        return;
    std::shared_ptr<ASTLiteral> literal = std::make_shared<ASTLiteral>(Field(table_info_.serialize()));
    if (args->children.size() == 1)
        args->children.emplace_back(literal);
    else if (args->children.size() == 2)
        args->children.back() = literal;
    else if (args->children.size() == 3)
        args->children.at(1) = literal;
    else
        throw Exception(
            fmt::format(
                "Wrong arguments num: {} in table: {} in modifyASTStorage",
                args->children.size(),
                this->getTableName()),
            ErrorCodes::BAD_ARGUMENTS);
}

// Used by `manage table xxx status` in ch-client
BlockInputStreamPtr StorageDeltaMerge::status()
{
    Block block;

    block.insert({std::make_shared<DataTypeString>(), "Name"});
    block.insert({std::make_shared<DataTypeString>(), "Value"});

    auto columns = block.mutateColumns();
    auto & name_col = columns[0];
    auto & value_col = columns[1];

    DM::StoreStats stat;
    if (storeInited())
    {
        stat = _store->getStoreStats();
    }

#define INSERT_INT(NAME)             \
    name_col->insert(String(#NAME)); \
    value_col->insert(DB::toString(stat.NAME));

#define INSERT_SIZE(NAME)            \
    name_col->insert(String(#NAME)); \
    value_col->insert(formatReadableSizeWithBinarySuffix(stat.NAME, 2));

#define INSERT_RATE(NAME)            \
    name_col->insert(String(#NAME)); \
    value_col->insert(DB::toString(stat.NAME * 100, 2) + "%");

#define INSERT_FLOAT(NAME)           \
    name_col->insert(String(#NAME)); \
    value_col->insert(DB::toString(stat.NAME, 2));

#define INSERT_STR(NAME)             \
    name_col->insert(String(#NAME)); \
    value_col->insert(stat.NAME);

    INSERT_INT(segment_count)
    INSERT_INT(total_rows)
    INSERT_SIZE(total_size)
    INSERT_INT(total_delete_ranges)

    INSERT_SIZE(total_delta_size)
    INSERT_SIZE(total_stable_size)

    INSERT_RATE(delta_rate_rows)
    INSERT_RATE(delta_rate_segments)

    INSERT_RATE(delta_placed_rate)
    INSERT_SIZE(delta_cache_size)
    INSERT_RATE(delta_cache_rate)
    INSERT_RATE(delta_cache_wasted_rate)

    INSERT_SIZE(delta_index_size)

    INSERT_FLOAT(avg_segment_rows)
    INSERT_SIZE(avg_segment_size)

    INSERT_INT(delta_count)
    INSERT_INT(total_delta_rows)
    INSERT_FLOAT(avg_delta_rows)
    INSERT_FLOAT(avg_delta_delete_ranges)

    INSERT_INT(stable_count)
    INSERT_INT(total_stable_rows)
    INSERT_FLOAT(avg_stable_rows)

    INSERT_INT(total_pack_count_in_delta)
    INSERT_INT(max_pack_count_in_delta)
    INSERT_FLOAT(avg_pack_count_in_delta)
    INSERT_FLOAT(avg_pack_rows_in_delta)
    INSERT_SIZE(avg_pack_size_in_delta)

    INSERT_INT(total_pack_count_in_stable)
    INSERT_FLOAT(avg_pack_count_in_stable)
    INSERT_FLOAT(avg_pack_rows_in_stable)
    INSERT_SIZE(avg_pack_size_in_stable)

    INSERT_INT(storage_stable_num_snapshots);
    INSERT_FLOAT(storage_stable_oldest_snapshot_lifetime);
    INSERT_INT(storage_stable_num_snapshots);
    INSERT_STR(storage_stable_oldest_snapshot_tracing_id);

    INSERT_INT(storage_delta_num_snapshots);
    INSERT_FLOAT(storage_delta_oldest_snapshot_lifetime);
    INSERT_INT(storage_delta_num_snapshots);
    INSERT_STR(storage_delta_oldest_snapshot_tracing_id);

    INSERT_INT(storage_meta_num_snapshots);
    INSERT_FLOAT(storage_meta_oldest_snapshot_lifetime);
    INSERT_INT(storage_meta_num_snapshots);
    INSERT_STR(storage_meta_oldest_snapshot_tracing_id);

    INSERT_INT(background_tasks_length);

#undef INSERT_INT
#undef INSERT_SIZE
#undef INSERT_RATE
#undef INSERT_FLOAT
#undef INSERT_STR

    return std::make_shared<OneBlockInputStream>(block);
}

void StorageDeltaMerge::startup()
{
    TMTContext & tmt = global_context.getTMTContext();
    tmt.getStorages().put(std::static_pointer_cast<StorageDeltaMerge>(shared_from_this()));
}

// Avoid calling virtual function `shutdown` in destructor,
// we should use this function instead.
// https://stackoverflow.com/a/12093250/4412495
void StorageDeltaMerge::shutdownImpl()
{
    bool v = false;
    if (!shutdown_called.compare_exchange_strong(v, true))
        return;
    if (storeInited())
    {
        _store->shutdown();
    }
}

void StorageDeltaMerge::shutdown()
{
    shutdownImpl();
}

void StorageDeltaMerge::removeFromTMTContext()
{
    // remove this table from TMTContext
    TMTContext & tmt_context = global_context.getTMTContext();
    auto keyspace_id = tidb_table_info.keyspace_id;
    auto table_id = tidb_table_info.id;
    tmt_context.getStorages().remove(keyspace_id, table_id);
    tmt_context.getRegionTable().removeTable(keyspace_id, table_id);
}

StorageDeltaMerge::~StorageDeltaMerge()
{
    shutdownImpl();
}

DataTypePtr StorageDeltaMerge::getPKTypeImpl() const
{
    if (storeInited())
    {
        return _store->getPKDataType();
    }
    std::lock_guard lock(store_mutex);
    if (storeInited())
    {
        return _store->getPKDataType();
    }
    return table_column_info->handle_column_define.type;
}

SortDescription StorageDeltaMerge::getPrimarySortDescription() const
{
    if (storeInited())
    {
        return _store->getPrimarySortDescription();
    }
    std::lock_guard lock(store_mutex);
    if (storeInited())
    {
        return _store->getPrimarySortDescription();
    }
    SortDescription desc;
    desc.emplace_back(table_column_info->handle_column_define.name, /* direction_= */ 1, /* nulls_direction_= */ 1);
    return desc;
}

DeltaMergeStorePtr & StorageDeltaMerge::getAndMaybeInitStore(ThreadPool * thread_pool)
{
    if (storeInited())
    {
        return _store;
    }
    std::lock_guard lock(store_mutex);
    if (_store == nullptr)
    {
        _store = std::make_shared<DeltaMergeStore>(
            global_context,
            data_path_contains_database_name,
            table_column_info->db_name,
            table_column_info->table_name,
            tidb_table_info.keyspace_id,
            tidb_table_info.id,
            tidb_table_info.replica_info.count > 0,
            std::move(table_column_info->table_column_defines),
            std::move(table_column_info->handle_column_define),
            is_common_handle,
            rowkey_column_size,
            DeltaMergeStore::Settings(),
            thread_pool);
        table_column_info.reset(nullptr);
        store_inited.store(true, std::memory_order_release);
    }
    return _store;
}

bool StorageDeltaMerge::initStoreIfDataDirExist(ThreadPool * thread_pool)
{
    if (shutdown_called.load(std::memory_order_relaxed) || isTombstone())
    {
        return false;
    }
    // If store is inited, we don't need to check data dir.
    if (storeInited())
    {
        return true;
    }
    if (!dataDirExist())
    {
        return false;
    }
    getAndMaybeInitStore(thread_pool);
    return true;
}

bool StorageDeltaMerge::dataDirExist()
{
    String db_name, table_name;
    {
        std::lock_guard lock(store_mutex);
        // store is inited after lock acquired.
        if (storeInited())
        {
            return true;
        }
        db_name = table_column_info->db_name;
        table_name = table_column_info->table_name;
    }

    auto path_pool = global_context.getPathPool().withTable(db_name, table_name, data_path_contains_database_name);
    auto path_delegate = path_pool.getStableDiskDelegator();
    for (const auto & root_path : path_delegate.listPaths())
    {
        int r = ::access(root_path.c_str(), F_OK);
        if (r == 0)
        {
            return true;
        }
    }
    return false;
}
} // namespace DB
