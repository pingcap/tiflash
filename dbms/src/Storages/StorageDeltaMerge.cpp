#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <Common/formatReadable.h>
#include <Common/typeid_cast.h>
#include <Core/Defines.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/isSupportedDataTypeCast.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTSelectQuery.h>
#include <Poco/File.h>
#include <Storages/AlterCommands.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/FilterParser/FilterParser.h>
#include <Storages/MutableSupport.h>
#include <Storages/PrimaryKeyNotMatchException.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDeltaMergeHelpers.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/SchemaNameMapper.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TypeMapping.h>
#include <common/ThreadPool.h>
#include <common/config_common.h>

#include <random>

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

StorageDeltaMerge::StorageDeltaMerge( //
    const String & db_engine,
    const String & db_name_,
    const String & table_name_,
    const OptionTableInfoConstRef table_info_,
    const ColumnsDescription & columns_,
    const ASTPtr & primary_expr_ast_,
    Timestamp tombstone,
    Context & global_context_)
    : IManageableStorage{columns_, tombstone},
      data_path_contains_database_name(db_engine != "TiFlash"),
      store_inited(false),
      max_column_id_used(0),
      global_context(global_context_.getGlobalContext()),
      log(&Logger::get("StorageDeltaMerge"))
{
    if (primary_expr_ast_->children.empty())
        throw Exception("No primary key");

    is_common_handle = false;
    pk_is_handle = false;
    // save schema from TiDB
    if (table_info_)
    {
        tidb_table_info = table_info_->get();
        is_common_handle = tidb_table_info.is_common_handle;
        pk_is_handle = tidb_table_info.pk_is_handle;
    }

    table_column_info = std::make_unique<TableColumnInfo>(db_name_, table_name_, primary_expr_ast_);

    updateTableColumnInfo();
}

void StorageDeltaMerge::updateTableColumnInfo()
{
    const ColumnsDescription & columns = getColumns();

    LOG_INFO(log,
        __FILE__ << " " << __func__ << " TableName " << table_column_info->table_name << " ordinary " << columns.ordinary.toString() << " materialized "
                 << columns.materialized.toString());

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
            auto itr = std::find_if(columns.begin(), columns.end(), [&](const ColumnInfo & v) { return v.id == col_def.id; });
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
                    throw Exception("pk column " + col.name + " is not representable by integer");
                }
                pks_combined_bytes += col.type->getSizeOfValueInMemory();
                if (pks_combined_bytes > sizeof(Handle))
                {
                    throw Exception("pk columns exceeds size limit :" + DB::toString(sizeof(Handle)));
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
            std::vector<String> actual_pri_keys;
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
            // fallover
        }
        // Unknown bug, throw an exception.
        std::stringstream ss;
        ss << "[";
        for (const auto & k : pks)
            ss << k << ",";
        ss << "]";
        std::stringstream columns_stream;
        columns_stream << "[";
        for (const auto & col : all_columns)
            columns_stream << col.name << ",";
        columns_stream << "]";
        throw Exception("Can not create table without primary key. Primary keys should be:" + ss.str()
            + ", but only these columns are found:" + columns_stream.str());
    }
    assert(!table_column_defines.empty());

    if (!(is_common_handle || pk_is_handle))
    {
        rowkey_column_defines.clear();
        rowkey_column_defines.push_back(handle_column_define);
    }
    rowkey_column_size = rowkey_column_defines.size();
}

void StorageDeltaMerge::drop()
{
    shutdown();
    if (storeInited())
    {
        _store->drop();
    }
}

Block StorageDeltaMerge::buildInsertBlock(bool is_import, bool is_delete, const Block & old_block)
{
    Block block = old_block;

    if (!is_import)
    {
        // Remove the default columns generated by InterpreterInsertQuery
        if (block.has(EXTRA_HANDLE_COLUMN_NAME))
            block.erase(EXTRA_HANDLE_COLUMN_NAME);
        if (block.has(VERSION_COLUMN_NAME))
            block.erase(VERSION_COLUMN_NAME);
        if (block.has(TAG_COLUMN_NAME))
            block.erase(TAG_COLUMN_NAME);
    }

    auto & store = getAndMaybeInitStore();
    const size_t rows = block.rows();
    if (!block.has(store->getHandle().name))
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
            auto & col = block.getByName(n);
            pk_columns.push_back(col.column);
            pk_column_types.push_back(col.type);
        }

        for (size_t c = 0; c < pk_column_count; ++c)
        {
            appendIntoHandleColumn(handle_data, pk_column_types[c], pk_columns[c]);
        }

        addColumnToBlock(block, EXTRA_HANDLE_COLUMN_ID, EXTRA_HANDLE_COLUMN_NAME,
            is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE, std::move(handle_column));
    }

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
        const DeltaMergeStorePtr & store_, const BlockDecorator & decorator_, const Context & db_context_, const Settings & db_settings_)
        : store(store_), header(store->getHeader()), decorator(decorator_), db_context(db_context_), db_settings(db_settings_)
    {}

    Block getHeader() const override { return *header; }

    void write(const Block & block) override
    try
    {
        if (db_settings.dt_insert_max_rows == 0)
        {
            store->write(db_context, db_settings, decorator(block));
        }
        else
        {
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

                store->write(db_context, db_settings, std::move(write_block));
            }
        }
    }
    catch (DB::Exception & e)
    {
        e.addMessage("(while writing to table `" + store->getDatabaseName() + "`.`" + store->getTableName() + "`)");
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
    auto & insert_query = typeid_cast<const ASTInsertQuery &>(*query);
    auto decorator = [&](const Block & block) { //
        return this->buildInsertBlock(insert_query.is_import, insert_query.is_delete, block);
    };
    return std::make_shared<DMBlockOutputStream>(getAndMaybeInitStore(), decorator, global_context, settings);
}

void StorageDeltaMerge::write(Block && block, const Settings & settings)
{
    auto & store = getAndMaybeInitStore();
#ifndef NDEBUG
    {
        // Do some check under DEBUG mode to ensure all block are written with column id properly set.
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
            else
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
            throw Exception("The column-id in written block is not properly set [name=" + name + "] [id=" + DB::toString(cid) + "]");
        }
    }
#endif

    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_during_write_to_storage);

    store->write(global_context, settings, std::move(block));
}

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

    throw Exception("Unable to parse segment IDs in literal form: `" + partition_ast.fields_str.toString() + "`");
}

BlockInputStreams StorageDeltaMerge::read( //
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
    auto header = store->getHeader();
    for (auto & n : column_names)
    {
        ColumnDefine col_define;
        if (n == EXTRA_HANDLE_COLUMN_NAME)
            col_define = store->getHandle();
        else if (n == VERSION_COLUMN_NAME)
            col_define = getVersionColumnDefine();
        else if (n == TAG_COLUMN_NAME)
            col_define = getTagColumnDefine();
        else
        {
            auto & column = header->getByName(n);
            col_define.name = column.name;
            col_define.id = column.column_id;
            col_define.type = column.type;
            col_define.default_value = column.default_value;
        }
        columns_to_read.push_back(col_define);
    }


    const ASTSelectQuery & select_query = typeid_cast<const ASTSelectQuery &>(*query_info.query);
    if (select_query.raw_for_mutable)
        return store->readRaw(
            context, context.getSettingsRef(), columns_to_read, num_streams, parseSegmentSet(select_query.segment_expression_list));
    else
    {
        if (unlikely(!query_info.mvcc_query_info))
            throw Exception("mvcc query info is null", ErrorCodes::LOGICAL_ERROR);

        TMTContext & tmt = context.getTMTContext();
        if (unlikely(!tmt.isInitialized()))
            throw Exception("TMTContext is not initialized", ErrorCodes::LOGICAL_ERROR);

        const auto & mvcc_query_info = *query_info.mvcc_query_info;

        LOG_DEBUG(log, "Read with tso: " << mvcc_query_info.read_tso);

        const auto check_read_tso = [&tmt, &context, this](UInt64 read_tso) {
            // Read with specify tso, check if tso is smaller than TiDB GcSafePoint
            auto pd_client = tmt.getPDClient();
            if (likely(!pd_client->isMock()))
            {
                auto safe_point = PDClientHelper::getGCSafePointWithRetry(pd_client,
                    /* ignore_cache= */ false,
                    global_context.getSettingsRef().safe_point_update_interval_seconds);
                if (read_tso < safe_point)
                    throw Exception("query id: " + context.getCurrentQueryId() + ", read tso: " + DB::toString(read_tso)
                            + " is smaller than tidb gc safe point: " + DB::toString(safe_point),
                        ErrorCodes::LOGICAL_ERROR);
            }
        };
        check_read_tso(mvcc_query_info.read_tso);

        String str_query_ranges;
        if (log->trace())
        {
            std::stringstream ss;
            for (const auto & region : mvcc_query_info.regions_query_info)
            {
                if (!region.required_handle_ranges.empty())
                {
                    for (const auto & range : region.required_handle_ranges)
                        ss << region.region_id << RecordKVFormat::DecodedTiKVKeyRangeToDebugString(range) << ",";
                }
                else
                {
                    /// only used for test cases
                    const auto & range = region.range_in_table;
                    ss << region.region_id << RecordKVFormat::DecodedTiKVKeyRangeToDebugString(range) << ",";
                }
            }
            str_query_ranges = ss.str();
        }

        auto ranges = getQueryRanges(mvcc_query_info.regions_query_info, tidb_table_info.id, is_common_handle, rowkey_column_size,
            /*expected_ranges_count*/ num_streams, log);

        if (log->trace())
        {
            std::stringstream ss_merged_range;
            for (const auto & range : ranges)
                ss_merged_range << range.toDebugString() << ",";
            LOG_TRACE(log, "reading ranges: orig, " << str_query_ranges << " merged, " << ss_merged_range.str());
        }

        /// Get Rough set filter from query
        DM::RSOperatorPtr rs_operator = DM::EMPTY_FILTER;
        const bool enable_rs_filter = context.getSettingsRef().dt_enable_rough_set_filter;
        if (enable_rs_filter)
        {
            if (likely(query_info.dag_query))
            {
                /// Query from TiDB / TiSpark
                auto create_attr_by_column_id = [this](ColumnID column_id) -> Attr {
                    const ColumnDefines & defines = this->getAndMaybeInitStore()->getTableColumns();
                    auto iter = std::find_if(
                        defines.begin(), defines.end(), [column_id](const ColumnDefine & d) -> bool { return d.id == column_id; });
                    if (iter != defines.end())
                        return Attr{.col_name = iter->name, .col_id = iter->id, .type = iter->type};
                    else
                        // Maybe throw an exception? Or check if `type` is nullptr before creating filter?
                        return Attr{.col_name = "", .col_id = column_id, .type = DataTypePtr{}};
                };
                rs_operator = FilterParser::parseDAGQuery(*query_info.dag_query, columns_to_read, std::move(create_attr_by_column_id), log);
            }
            else
            {
#if 0
                // Query from ch client
                auto create_attr_by_column_id = [this](const String & col_name) -> Attr {
                    const ColumnDefines & defines = this->getAndMaybeInitStore()->getTableColumns();
                    auto iter = std::find_if(
                        defines.begin(), defines.end(), [&col_name](const ColumnDefine & d) -> bool { return d.name == col_name; });
                    if (iter != defines.end())
                        return Attr{.col_name = iter->name, .col_id = iter->id, .type = iter->type};
                    else
                        // Maybe throw an exception? Or check if `type` is nullptr before creating filter?
                        return Attr{.col_name = col_name, .col_id = 0, .type = DataTypePtr{}};
                };
                rs_operator = FilterParser::parseSelectQuery(select_query, std::move(create_attr_by_column_id), log);
#endif
            }
            if (likely(rs_operator != DM::EMPTY_FILTER))
                LOG_DEBUG(log, "Rough set filter: " << rs_operator->toDebugString());
        }
        else
            LOG_DEBUG(log, "Rough set filter is disabled.");

        auto streams = store->read(context, context.getSettingsRef(), columns_to_read, ranges, num_streams,
            /*max_version=*/mvcc_query_info.read_tso, rs_operator, max_block_size, parseSegmentSet(select_query.segment_expression_list));

        /// Ensure read_tso info after read.
        check_read_tso(mvcc_query_info.read_tso);

        LOG_TRACE(log, "[ranges: " << ranges.size() << "] [streams: " << streams.size() << "]");

        return streams;
    }
}

void StorageDeltaMerge::checkStatus(const Context & context) { getAndMaybeInitStore()->check(context); }

void StorageDeltaMerge::flushCache(const Context & context)
{
    flushCache(context, DM::RowKeyRange::newAll(is_common_handle, rowkey_column_size));
}

void StorageDeltaMerge::flushCache(const Context & context, const DM::RowKeyRange & range_to_flush)
{
    getAndMaybeInitStore()->flushCache(context, range_to_flush);
}

void StorageDeltaMerge::mergeDelta(const Context & context) { getAndMaybeInitStore()->mergeDeltaAll(context); }

void StorageDeltaMerge::deleteRange(const DM::RowKeyRange & range_to_delete, const Settings & settings)
{
    auto metrics = global_context.getTiFlashMetrics();
    GET_METRIC(metrics, tiflash_storage_command_count, type_delete_range).Increment();
    return getAndMaybeInitStore()->deleteRange(global_context, settings, range_to_delete);
}

void StorageDeltaMerge::ingestFiles(
    const DM::RowKeyRange & range, const std::vector<UInt64> & file_ids, bool clear_data_in_range, const Settings & settings)
{
    auto metrics = global_context.getTiFlashMetrics();
    GET_METRIC(metrics, tiflash_storage_command_count, type_ingest).Increment();
    return getAndMaybeInitStore()->ingestFiles(global_context, settings, range, file_ids, clear_data_in_range);
}

UInt64 StorageDeltaMerge::onSyncGc(Int64 limit)
{
    if (storeInited())
    {
        return _store->onSyncGc(limit);
    }
    return 0;
}

size_t getRows(DM::DeltaMergeStorePtr & store, const Context & context, const DM::RowKeyRange & range)
{
    size_t rows = 0;

    ColumnDefines to_read{getExtraHandleColumnDefine(store->isCommonHandle())};
    auto stream = store->read(context, context.getSettingsRef(), to_read, {range}, 1, MAX_UINT64, EMPTY_FILTER)[0];
    stream->readPrefix();
    Block block;
    while ((block = stream->read()))
        rows += block.rows();
    stream->readSuffix();

    return rows;
}

DM::RowKeyRange getRange(DM::DeltaMergeStorePtr & store, const Context & context, size_t total_rows, size_t delete_rows)
{
    auto start_index = rand() % (total_rows - delete_rows + 1);

    DM::RowKeyRange range = DM::RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize());
    {
        ColumnDefines to_read{getExtraHandleColumnDefine(store->isCommonHandle())};
        auto stream = store->read(context, context.getSettingsRef(), to_read,
            {DM::RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())}, 1, MAX_UINT64, EMPTY_FILTER)[0];
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
        LOG_ERROR(log, "Expected delete rows: " << delete_rows << ", got: " << actual_delete_rows);

    store->deleteRange(context, context.getSettingsRef(), delete_range);

    size_t after_delete_rows = getRows(store, context, DM::RowKeyRange::newAll(is_common_handle, rowkey_column_size));
    if (after_delete_rows != total_rows - delete_rows)
        LOG_ERROR(log, "Rows after delete range not match, expected: " << (total_rows - delete_rows) << ", got: " << after_delete_rows);
}

//==========================================================================================
// DDL methods.
//==========================================================================================
void StorageDeltaMerge::alterFromTiDB(const TableLockHolder &, const AlterCommands & params, const String & database_name,
    const TiDB::TableInfo & table_info, const SchemaNameMapper & name_mapper, const Context & context)
{
    alterImpl(params, database_name, name_mapper.mapTableName(table_info),
        std::optional<std::reference_wrapper<const TiDB::TableInfo>>(table_info), context);
}

void StorageDeltaMerge::alter(const TableLockHolder &, const AlterCommands & commands, const String & database_name,
    const String & table_name_, const Context & context)
{
    alterImpl(commands, database_name, table_name_, std::nullopt, context);
}

/// If any ddl statement change StorageDeltaMerge's schema,
/// we need to update the create statement in metadata, so that we can restore table structure next time
static void updateDeltaMergeTableCreateStatement(            //
    const String & database_name, const String & table_name, //
    const SortDescription & pk_names, const ColumnsDescription & columns,
    const OrderedNameSet & hidden_columns,    //
    const OptionTableInfoConstRef table_info, //
    Timestamp tombstone, const Context & context);

inline OptionTableInfoConstRef getTableInfoForCreateStatement( //
    const OptionTableInfoConstRef table_info_from_tidb,        //
    TiDB::TableInfo & table_info_from_store, const ColumnDefines & store_table_columns, const OrderedNameSet & hidden_columns)
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
            NameAndTypePair{column_define.name, column_define.type}, column_define.id, column_define.default_value,
            /* for_test= */ true);
        table_info_from_store.columns.emplace_back(std::move(column_info));
    }
    return std::optional<std::reference_wrapper<const TiDB::TableInfo>>(table_info_from_store);
}

void StorageDeltaMerge::alterImpl(const AlterCommands & commands,
    const String & database_name,
    const String & table_name_,
    const OptionTableInfoConstRef table_info,
    const Context & context)
try
{
    std::unordered_set<String> cols_drop_forbidden;
    cols_drop_forbidden.insert(EXTRA_HANDLE_COLUMN_NAME);
    cols_drop_forbidden.insert(VERSION_COLUMN_NAME);
    cols_drop_forbidden.insert(TAG_COLUMN_NAME);

    auto tombstone = getTombstone();

    for (const auto & command : commands)
    {
        if (command.type == AlterCommand::MODIFY_PRIMARY_KEY)
        {
            // check that add primary key is forbidden
            throw Exception("Storage engine " + getName() + " doesn't support modify primary key.", ErrorCodes::BAD_ARGUMENTS);
        }
        else if (command.type == AlterCommand::DROP_COLUMN)
        {
            // check that drop hidden columns is forbidden
            if (cols_drop_forbidden.count(command.column_name) > 0)
                throw Exception("Storage engine " + getName() + " doesn't support drop hidden column: " + command.column_name,
                    ErrorCodes::BAD_ARGUMENTS);
        }
        else if (command.type == AlterCommand::TOMBSTONE)
        {
            tombstone = command.tombstone;
        }
        else if (command.type == AlterCommand::RECOVER)
        {
            tombstone = 0;
        }
    }

    // update the metadata in database, so that we can read the new schema using TiFlash's client
    ColumnsDescription new_columns = getColumns();
    for (const auto & command : commands)
    {
        if (command.type == AlterCommand::MODIFY_COLUMN)
        {
            // find the column we are going to modify
            auto col_iter = command.findColumn(new_columns.ordinary); // just find in ordinary columns
            if (unlikely(!isSupportedDataTypeCast(col_iter->type, command.data_type)))
            {
                // If this table has no tiflash replica, simply ignore this check because TiDB constraint
                // on DDL is not strict. (https://github.com/pingcap/tidb/issues/17530)
                // If users applied unsupported column type change on table with tiflash replica. To get rid of
                // this exception and avoid of reading broken data, they have truncate that table.
                if (table_info && table_info.value().get().replica_info.count == 0)
                {
                    LOG_WARNING(log,
                        "Accept lossy column data type modification. Table (id:" + DB::toString(table_info.value().get().id)
                            + ") modify column " + command.column_name + "(" + DB::toString(command.column_id) + ") from "
                            + col_iter->type->getName() + " to " + command.data_type->getName());
                }
                else
                {
                    // check that lossy changes is forbidden
                    // check that changing the UNSIGNED attribute is forbidden
                    throw Exception("Storage engine " + getName() + " doesn't support lossy data type modification. Try to modify column "
                            + command.column_name + "(" + DB::toString(command.column_id) + ") from " + col_iter->type->getName() + " to "
                            + command.data_type->getName(),
                        ErrorCodes::NOT_IMPLEMENTED);
                }
            }
        }
    }

    commands.apply(new_columns); // apply AlterCommands to `new_columns`
    setColumns(std::move(new_columns));
    if (table_info)
    {
        tidb_table_info = table_info.value();
    }

    {
        std::lock_guard lock(store_mutex);  // Avoid concurrent init store and DDL.
        if (storeInited())
        {
            _store->applyAlters(commands, table_info, max_column_id_used, context);
        }
        else
        {
            updateTableColumnInfo();
        }
    }

    SortDescription pk_desc = getPrimarySortDescription();
    ColumnDefines store_columns = getStoreColumnDefines();
    TiDB::TableInfo table_info_from_store;
    table_info_from_store.name = table_name_;
    // after update `new_columns` and store's table columns, we need to update create table statement,
    // so that we can restore table next time.
    updateDeltaMergeTableCreateStatement(database_name, table_name_, pk_desc, getColumns(), hidden_columns,
        getTableInfoForCreateStatement(table_info, table_info_from_store, store_columns, hidden_columns), tombstone, context);
    setTombstone(tombstone);
}
catch (Exception & e)
{
    String table_info_msg;
    if (table_info)
        table_info_msg = " table name: " + table_name_ + ", table id: " + DB::toString(table_info.value().get().id);
    else
        table_info_msg = " table name: " + table_name_ + ", table id: unknown";
    e.addMessage(table_info_msg);
    throw;
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
        if (col.id != table_column_info->handle_column_define.id && col.id != VERSION_COLUMN_ID && col.id != TAG_COLUMN_ID)
        {
            cols.emplace_back(col);
        }
    }
    return cols;
}

String StorageDeltaMerge::getName() const { return MutableSupport::delta_tree_storage_name; }

void StorageDeltaMerge::rename(
    const String & new_path_to_db, const String & new_database_name, const String & new_table_name, const String & new_display_table_name)
{
    tidb_table_info.name = new_display_table_name; // update name in table info
    // For DatabaseTiFlash, simply update store's database is OK.
    // `store->getTableName() == new_table_name` only keep for mock test.
    bool clean_rename = !data_path_contains_database_name && getTableName() == new_table_name;
    if (likely(clean_rename))
    {
        if (storeInited())
        {
            _store->rename(new_path_to_db, clean_rename, new_database_name, new_table_name);
            return;
        }
        std::lock_guard lock(store_mutex);
        if (storeInited())
        {
            _store->rename(new_path_to_db, clean_rename, new_database_name, new_table_name);
        }
        else
        {
            table_column_info->db_name = new_database_name;
            table_column_info->table_name = new_table_name;
        }
        return;
    }

    /// Note that this routine is only left for CI tests. `clean_rename` should always be true in production env.
    auto & store = getAndMaybeInitStore();

    // For DatabaseOrdinary, we need to rename data path, then recreate a new store.
    const String new_path = new_path_to_db + "/" + new_table_name;

    if (Poco::File{new_path}.exists())
        throw Exception{"Target path already exists: " + new_path,
            /// @todo existing target can also be a file, not directory
            ErrorCodes::DIRECTORY_ALREADY_EXISTS};

    // flush store and then reset store to new path
    store->flushCache(global_context, RowKeyRange::newAll(is_common_handle, rowkey_column_size));
    ColumnDefines table_column_defines = store->getTableColumns();
    ColumnDefine handle_column_define = store->getHandle();
    DeltaMergeStore::Settings settings = store->getSettings();

    // remove background tasks
    store->shutdown();
    // rename directories for multi disks
    store->rename(new_path, clean_rename, new_database_name, new_table_name);
    // generate a new store
    store = std::make_shared<DeltaMergeStore>(global_context,                //
        data_path_contains_database_name, new_database_name, new_table_name, //
        std::move(table_column_defines), std::move(handle_column_define), is_common_handle, rowkey_column_size, settings);
}

String StorageDeltaMerge::getTableName() const
{
    if (storeInited())
    {
        return _store->getTableName();
    }
    std::lock_guard lock(store_mutex);
    if (storeInited())
    {
        return _store->getTableName();
    }
    return table_column_info->table_name;
}

String StorageDeltaMerge::getDatabaseName() const
{
    if (storeInited())
    {
        return _store->getDatabaseName();
    }
    std::lock_guard lock(store_mutex);
    if (storeInited())
    {
        return _store->getDatabaseName();
    }
    return table_column_info->db_name;
}

void updateDeltaMergeTableCreateStatement(                   //
    const String & database_name, const String & table_name, //
    const SortDescription & pk_names, const ColumnsDescription & columns,
    const OrderedNameSet & hidden_columns,    //
    const OptionTableInfoConstRef table_info, //
    Timestamp tombstone, const Context & context)
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
                throw Exception("Try to update table(" + database_name + "." + table_name + ") statement with no primary key. ");
            }
        }

        std::shared_ptr<ASTLiteral> tableinfo_literal = std::make_shared<ASTLiteral>(Field(table_info->get().serialize()));
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
            throw Exception("Wrong arguments num:" + DB::toString(args.children.size()) + " in table: " + table_name
                    + " with engine=" + MutableSupport::delta_tree_storage_name,
                ErrorCodes::BAD_ARGUMENTS);
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
            "Wrong arguments num: " + DB::toString(args->children.size()) + " in table: " + this->getTableName() + " in modifyASTStorage",
            ErrorCodes::BAD_ARGUMENTS);
}

BlockInputStreamPtr StorageDeltaMerge::status()
{
    Block block;

    block.insert({std::make_shared<DataTypeString>(), "Name"});
    block.insert({std::make_shared<DataTypeString>(), "Value"});

    auto columns = block.mutateColumns();
    auto & name_col = columns[0];
    auto & value_col = columns[1];

    DeltaMergeStoreStat stat;
    if (storeInited())
    {
        stat = _store->getStat();
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
    INSERT_FLOAT(avg_pack_count_in_delta)
    INSERT_FLOAT(avg_pack_rows_in_delta)
    INSERT_SIZE(avg_pack_size_in_delta)

    INSERT_INT(total_pack_count_in_stable)
    INSERT_FLOAT(avg_pack_count_in_stable)
    INSERT_FLOAT(avg_pack_rows_in_stable)
    INSERT_SIZE(avg_pack_size_in_stable)

    INSERT_INT(storage_stable_num_snapshots);
    INSERT_INT(storage_stable_num_pages);
    INSERT_INT(storage_stable_num_normal_pages)
    INSERT_INT(storage_stable_max_page_id);

    INSERT_INT(storage_delta_num_snapshots);
    INSERT_INT(storage_delta_num_pages);
    INSERT_INT(storage_delta_num_normal_pages)
    INSERT_INT(storage_delta_max_page_id);

    INSERT_INT(storage_meta_num_snapshots);
    INSERT_INT(storage_meta_num_pages);
    INSERT_INT(storage_meta_num_normal_pages)
    INSERT_INT(storage_meta_max_page_id);

    INSERT_INT(background_tasks_length);

#undef INSERT_INT
#undef INSERT_SIZE
#undef INSERT_RATE
#undef INSERT_FLOAT

    return std::make_shared<OneBlockInputStream>(block);
}

void StorageDeltaMerge::startup()
{
    TMTContext & tmt = global_context.getTMTContext();
    tmt.getStorages().put(std::static_pointer_cast<StorageDeltaMerge>(shared_from_this()));
}

void StorageDeltaMerge::shutdown()
{
    bool v = false;
    if (!shutdown_called.compare_exchange_strong(v, true))
        return;
    if (storeInited())
    {
        _store->shutdown();
    }
}

void StorageDeltaMerge::removeFromTMTContext()
{
    // remove this table from TMTContext
    TMTContext & tmt_context = global_context.getTMTContext();
    tmt_context.getStorages().remove(tidb_table_info.id);
    tmt_context.getRegionTable().removeTable(tidb_table_info.id);
}

StorageDeltaMerge::~StorageDeltaMerge() { shutdown(); }

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

DeltaMergeStorePtr & StorageDeltaMerge::getAndMaybeInitStore()
{
    if (storeInited())
    {
        return _store;
    }
    std::lock_guard<std::mutex> lock(store_mutex);
    if (_store == nullptr)
    {
        _store = std::make_shared<DeltaMergeStore>(global_context, data_path_contains_database_name, table_column_info->db_name,
            table_column_info->table_name, std::move(table_column_info->table_column_defines),
            std::move(table_column_info->handle_column_define), is_common_handle, rowkey_column_size, DeltaMergeStore::Settings());
        table_column_info.reset(nullptr);
        store_inited.store(true, std::memory_order_release);
    }
    return _store;
}

bool StorageDeltaMerge::initStoreIfDataDirExist()
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
    getAndMaybeInitStore();
    return true;
}

bool StorageDeltaMerge::dataDirExist()
{
    String db_name, table_name;
    {
        std::lock_guard<std::mutex> lock(store_mutex);
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
