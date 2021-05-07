#include <optional>
#include <Common/FailPoint.h>
#include <Common/FieldVisitors.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/TxnMergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/DiskSpaceMonitor.h>
#include <Storages/MergeTree/MergeList.h>
#include <Databases/IDatabase.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/PartLog.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>

#include <Storages/MutableSupport.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTOptimizeQuery.h>

#include <DataStreams/RemoveColumnsBlockInputStream.h>
#include <DataStreams/AdditionalColumnsBlockOutputStream.h>
#include <DataStreams/DedupSortedBlockInputStream.h>
#include <DataStreams/InBlockDedupBlockOutputStream.h>

#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>
#include <Storages/Transaction/SchemaNameMapper.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDB.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int INCORRECT_FILE_NAME;
    extern const int CANNOT_ASSIGN_OPTIMIZE;
    extern const int FAIL_POINT_ERROR;
}

namespace FailPoints
{
extern const char exception_between_alter_data_and_meta[];
}


StorageMergeTree::StorageMergeTree(const String & path_,
    const String & db_engine_,
    const String & database_name_,
    const String & table_name_,
    const ColumnsDescription & columns_,
    bool attach,
    Context & context_,
    const TableInfo & table_info_,
    const ASTPtr & primary_expr_ast_,
    const ASTPtr & secondary_sorting_expr_list_,
    const String & date_column_name,
    const ASTPtr & partition_expr_ast_,
    const ASTPtr & sampling_expression_, /// nullptr, if sampling is not supported.
    const MergeTreeData::MergingParams & merging_params_,
    const MergeTreeSettings & settings_,
    bool has_force_restore_data_flag,
    Timestamp tombstone)
    : IManageableStorage{tombstone},
      path(path_),
      data_path_contains_database_name(db_engine_ != "TiFlash"),
      database_name(database_name_),
      table_name(table_name_),
      full_path(path + escapeForFileName(table_name) + '/'),
      context(context_),
      background_pool(context_.getBackgroundPool()),
      data(database_name, table_name, full_path, data_path_contains_database_name, columns_, context_, primary_expr_ast_,
          secondary_sorting_expr_list_, date_column_name, partition_expr_ast_, sampling_expression_, merging_params_, settings_, false,
          attach),
      reader(data),
      writer(data),
      merger(data, context.getBackgroundPool()),
      log(&Logger::get(database_name_ + "." + table_name + " (StorageMergeTree)"))
{
    *data.table_info = table_info_;
    if (path_.empty())
        throw Exception("MergeTree storages require data path", ErrorCodes::INCORRECT_FILE_NAME);

    data.loadDataParts(has_force_restore_data_flag);

    if (!attach && !data.getDataParts().empty())
        throw Exception("Data directory for table already containing data parts - probably it was unclean DROP table or manual intervention. You must either clear directory by hand or use ATTACH TABLE instead of CREATE TABLE if you need to use that parts.", ErrorCodes::INCORRECT_DATA);

    increment.set(data.getMaxDataPartIndex());
}


void StorageMergeTree::startup()
{
    if (data.merging_params.mode == MergeTreeData::MergingParams::Txn)
    {
        TMTContext & tmt = context.getTMTContext();
        tmt.getStorages().put(std::static_pointer_cast<StorageMergeTree>(shared_from_this()));
    }

    merge_task_handle = background_pool.addTask([this] { return mergeTask(); });

    data.clearOldPartsFromFilesystem();

    /// Temporary directories contain incomplete results of merges (after forced restart)
    ///  and don't allow to reinitialize them, so delete each of them immediately
    data.clearOldTemporaryDirectories(0);
}


void StorageMergeTree::shutdown()
{
    if (shutdown_called)
        return;
    shutdown_called = true;
    merger.merges_blocker.cancelForever();
    if (merge_task_handle)
        background_pool.removeTask(merge_task_handle);
}

void StorageMergeTree::removeFromTMTContext()
{
    // The drop table operation may be failed and retry.
    // Before the storage is dropped successfully, we should not drop it from TMT.
    if (data.merging_params.mode == MergeTreeData::MergingParams::Txn)
    {
        TMTContext & tmt_context = context.getTMTContext();
        tmt_context.getStorages().remove(data.table_info->id);
        tmt_context.getRegionTable().removeTable(data.table_info->id);
    }
};

StorageMergeTree::~StorageMergeTree()
{
    shutdown();
}

BlockInputStreams StorageMergeTree::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    auto res = reader.read(column_names, query_info, context, processed_stage, max_block_size, num_streams, 0);
    const ASTSelectQuery * select_query = typeid_cast<const ASTSelectQuery *>(query_info.query.get());

    if (data.merging_params.mode == MergeTreeData::MergingParams::Mutable ||
        data.merging_params.mode == MergeTreeData::MergingParams::Txn)
    {
        if (select_query && !select_query->raw_for_mutable)
        {
            LOG_DEBUG(log, "Mutable table dedup read.");
            Names filtered_names;
            filtered_names.push_back(MutableSupport::version_column_name);
            filtered_names.push_back(MutableSupport::delmark_column_name);
            for (size_t i = 0; i < res.size(); ++i)
                res[i] = std::make_shared<RemoveColumnsBlockInputStream>(res[i], filtered_names);
        }
        else
            LOG_DEBUG(log, "Mutable table raw read.");
    }
    else if (select_query && select_query->raw_for_mutable)
    {
        throw Exception("Only " + MutableSupport::mmt_storage_name + " support SELRAW.", ErrorCodes::BAD_ARGUMENTS);
    }
    return res;
}

UInt64 StorageMergeTree::getVersionSeed(const Settings & /* settings */) const
{
    // NOTE:
    // We don't guarantee thread safe.
    // In that case, we assume data not related in diffent thread.
    // So we can use the same version number.
    //
    // WARNING: NOT safe in distribed mode.

    // TODO:
    // It's faster if we use (increment.value << 32).
    // We use '* 1000000' for readable results, just for now
    return (increment.value + 1) * 1000000;
}

const StorageMergeTree::TableInfo & StorageMergeTree::getTableInfo() const { return *data.table_info; }

void StorageMergeTree::setTableInfo(const TableInfo & table_info_) { *data.table_info = table_info_; }

BlockOutputStreamPtr StorageMergeTree::write(const ASTPtr & query, const Settings & settings)
{
    BlockOutputStreamPtr res = std::make_shared<MergeTreeBlockOutputStream>(*this);

    const ASTInsertQuery * insert_query = typeid_cast<const ASTInsertQuery *>(&*query);
    const ASTDeleteQuery * delete_query = typeid_cast<const ASTDeleteQuery *>(&*query);

    if (data.merging_params.mode == MergeTreeData::MergingParams::Txn)
    {
        res = std::make_shared<TxnMergeTreeBlockOutputStream>(*this);

        if (insert_query)
        {
            if (!insert_query->is_import)
            {
                LOG_DEBUG(log, "TMT table writing, add version column and del-mark column.");
                AdditionalBlockGenerators gens
                {
                    std::make_shared<AdditionalBlockGeneratorPD>(MutableSupport::version_column_name,
                        context.getTMTContext().getPDClient()),
                    std::make_shared<AdditionalBlockGeneratorConst<UInt8>>(MutableSupport::delmark_column_name,
                        MutableSupport::DelMark::genDelMark(false))
                };
                res = std::make_shared<AdditionalColumnsBlockOutputStream>(res, gens);
                res = std::make_shared<InBlockDedupBlockOutputStream>(res, data.getSortDescription());
            }
            else
                LOG_DEBUG(log, "TMT table importing.");
        }
        else if (delete_query)
        {
            LOG_DEBUG(log, "Delete from TMT table, add version column and del-mark column.");
            AdditionalBlockGenerators gens
            {
                std::make_shared<AdditionalBlockGeneratorPD>(MutableSupport::version_column_name,
                    context.getTMTContext().getPDClient()),
                std::make_shared<AdditionalBlockGeneratorConst<UInt8>>(MutableSupport::delmark_column_name,
                    MutableSupport::DelMark::genDelMark(true))
            };
            res = std::make_shared<AdditionalColumnsBlockOutputStream>(res, gens);
            res = std::make_shared<InBlockDedupBlockOutputStream>(res, data.getSortDescription());
        }
        else
        {
            throw Exception("Use TMT table by wrong way", ErrorCodes::BAD_ARGUMENTS);
        }
    }
    else if (data.merging_params.mode == MergeTreeData::MergingParams::Mutable)
    {
        if (insert_query)
        {
            if (!insert_query->is_import)
            {
                LOG_DEBUG(log, "Mutable table writing, add version column and del-mark column.");
                AdditionalBlockGenerators gens
                {
                    std::make_shared<AdditionalBlockGeneratorIncrease<UInt64>>(MutableSupport::version_column_name,
                        getVersionSeed(settings)),
                    std::make_shared<AdditionalBlockGeneratorConst<UInt8>>(MutableSupport::delmark_column_name,
                        MutableSupport::DelMark::genDelMark(false))
                };
                res = std::make_shared<AdditionalColumnsBlockOutputStream>(res, gens);
                // TODO: Dedup in MergeTreeDataWriter may be better, in case some one glue some blocks together
                res = std::make_shared<InBlockDedupBlockOutputStream>(res, data.getSortDescription());
            }
            else
                LOG_DEBUG(log, "Mutable table importing.");
        }
        else if (delete_query)
        {
            LOG_DEBUG(log, "Mutable table deleting.");
            AdditionalBlockGenerators gens
            {
                std::make_shared<AdditionalBlockGeneratorConst<UInt64>>(MutableSupport::version_column_name,
                    getVersionSeed(settings)),
                std::make_shared<AdditionalBlockGeneratorConst<UInt8>>(MutableSupport::delmark_column_name,
                    MutableSupport::DelMark::genDelMark(true))
            };
            res = std::make_shared<AdditionalColumnsBlockOutputStream>(res, gens);
            res = std::make_shared<InBlockDedupBlockOutputStream>(res, data.getSortDescription());
        }
    }
    else if ((insert_query && insert_query->is_import) || delete_query)
    {
        throw Exception("Only " + MutableSupport::mmt_storage_name + " support IMPORT or DELETE.", ErrorCodes::BAD_ARGUMENTS);
    }

    return res;
}

bool StorageMergeTree::checkTableCanBeDropped() const
{
    const_cast<MergeTreeData &>(getData()).recalculateColumnSizes();
    context.checkTableCanBeDropped(database_name, table_name, getData().getTotalActiveSizeInBytes());
    return true;
}

const OrderedNameSet & StorageMergeTree::getHiddenColumnsImpl() const
{
    return MutableSupport::instance().hiddenColumns(getName());
}

void StorageMergeTree::drop()
{
    shutdown();
    data.dropAllData();
}

void StorageMergeTree::rename(
    const String & new_path_to_db, const String & new_database_name, const String & new_table_name, const String & new_display_table_name)
{
    std::string new_full_path = new_path_to_db + escapeForFileName(new_table_name) + '/';

    data.setPath(new_full_path);

    for (auto & path : context.getPartPathSelector().getAllPath())
    {
        std::string orig_parts_path = path + "data/";
        std::string new_parts_path = path + "data/";
        if (data_path_contains_database_name)
        {
            orig_parts_path += escapeForFileName(data.database_name) + '/';
            new_parts_path += escapeForFileName(new_database_name) + '/';
        }
        orig_parts_path += escapeForFileName(data.table_name) + '/';
        new_parts_path += escapeForFileName(new_table_name) + '/';
        if (data_path_contains_database_name && Poco::File{new_parts_path}.exists())
            throw Exception{"Target path already exists: " + new_parts_path,
                /// @todo existing target can also be a file, not directory
                ErrorCodes::DIRECTORY_ALREADY_EXISTS};
        else
            Poco::File(orig_parts_path).renameTo(new_parts_path);
    }
    context.dropCaches();
    path = new_path_to_db;
    table_name = new_table_name;
    database_name = new_database_name;
    full_path = new_full_path;

    data.table_name = new_table_name;
    data.database_name = new_database_name;
    data.table_info->name = new_display_table_name; // update name in table info
    /// NOTE: Logger names are not updated.
}

void StorageMergeTree::alter(
    const TableLockHolder &,
    const AlterCommands & params,
    const String & database_name,
    const String & table_name,
    const Context & context)
{
    alterInternal(params, database_name, table_name, std::nullopt, context);
}

void StorageMergeTree::alterFromTiDB(
    const TableLockHolder &,
    const AlterCommands & params,
    const String & database_name,
    const TiDB::TableInfo & table_info,
    const SchemaNameMapper & name_mapper,
    const Context & context)
{
    alterInternal(params, database_name, name_mapper.mapTableName(table_info), std::optional<std::reference_wrapper<const TableInfo>>(table_info), context);
}

void StorageMergeTree::alterInternal(
    const AlterCommands & params,
    const String & database_name,
    const String & table_name,
    const std::optional<std::reference_wrapper<const TiDB::TableInfo>> table_info,
    const Context & context)
{
    /// NOTE: Here, as in ReplicatedMergeTree, you can do ALTER which does not block the writing of data for a long time.
    auto merge_blocker = merger.merges_blocker.cancel();

    data.checkAlter(params);

    auto new_columns = data.getColumns();
    params.apply(new_columns);

    std::vector<MergeTreeData::AlterDataPartTransactionPtr> transactions;

    bool primary_key_is_modified = false;

    ASTPtr new_primary_key_ast = data.primary_expr_ast;

    bool rename_column = false;

    std::optional<Timestamp> tombstone = std::nullopt;

    for (const AlterCommand & param : params)
    {
        if (param.type == AlterCommand::MODIFY_PRIMARY_KEY)
        {
            primary_key_is_modified = true;
            new_primary_key_ast = param.primary_key;
        }
        else if (param.type == AlterCommand::RENAME_COLUMN)
        {
            rename_column = true;
            if (param.primary_key != nullptr)
            {
                LOG_INFO(log, "old pk: " << *new_primary_key_ast);
                new_primary_key_ast = param.primary_key;
                LOG_INFO(log, "change to new pk: " << *new_primary_key_ast);
            }
            if (params.size() != 1)
            {
                throw Exception("There is an internal error for rename columns, params size is " + std::to_string(params.size()) + ", but should be 1", ErrorCodes::LOGICAL_ERROR);
            }
        }
        else if (param.type == AlterCommand::TOMBSTONE)
        {
            tombstone = param.tombstone;
        }
        else if (param.type == AlterCommand::RECOVER)
        {
            tombstone = 0;
        }
    }

    if (primary_key_is_modified && supportsSampling())
        throw Exception("MODIFY PRIMARY KEY only supported for tables without sampling key", ErrorCodes::BAD_ARGUMENTS);

    auto parts = data.getDataParts({MergeTreeDataPartState::PreCommitted, MergeTreeDataPartState::Committed, MergeTreeDataPartState::Outdated});
    auto columns_for_parts = new_columns.getAllPhysical();
    for (const MergeTreeData::DataPartPtr & part : parts)
    {
        if (rename_column)
        {
            auto transaction = data.renameColumnPart(part, columns_for_parts, params[0]);
            if (transaction != nullptr)
                transactions.push_back(std::move(transaction));
        }
        else if (auto transaction = data.alterDataPart(part, columns_for_parts, new_primary_key_ast, false))
            transactions.push_back(std::move(transaction));
    }

    IDatabase::ASTModifier storage_modifier = [primary_key_is_modified, new_primary_key_ast, table_info, tombstone] (IAST & ast)
    {
        auto & storage_ast = typeid_cast<ASTStorage &>(ast);

        if (primary_key_is_modified)
        {
            auto tuple = std::make_shared<ASTFunction>();
            tuple->name = "tuple";
            tuple->arguments = new_primary_key_ast;
            tuple->children.push_back(tuple->arguments);

            /// Primary key is in the second place in table engine description and can be represented as a tuple.
            /// TODO: Not always in second place. If there is a sampling key, then the third one. Fix it.
            typeid_cast<ASTExpressionList &>(*storage_ast.engine->arguments).children.at(1) = tuple;
        }

        if (table_info)
        {
            auto literal = std::make_shared<ASTLiteral>(Field(table_info->get().serialize()));
            typeid_cast<ASTExpressionList &>(*storage_ast.engine->arguments).children.at(2) = literal;
        }

        if (tombstone)
        {
            auto tombstone_ast = std::make_shared<ASTLiteral>(Field(tombstone.value()));
            if (storage_ast.engine->arguments->children.size() == 3)
                typeid_cast<ASTExpressionList &>(*storage_ast.engine->arguments).children.emplace_back(tombstone_ast);
            else
                typeid_cast<ASTExpressionList &>(*storage_ast.engine->arguments).children.at(3) = tombstone_ast;
        }
    };


    for (auto & transaction : transactions)
        transaction->commit();

    // The process of data change and meta change is not atomic, so we must make sure change data firstly
    // and change meta secondly. If server crashes during or after changing data, we must fix the schema after restart.
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_between_alter_data_and_meta);

    context.getDatabase(database_name)->alterTable(context, table_name, new_columns, storage_modifier);
    setColumns(std::move(new_columns));
    if (table_info)
        setTableInfo(table_info->get());
    if (tombstone)
        setTombstone(tombstone.value());

    if (new_primary_key_ast != nullptr)
    {
        data.primary_expr_ast = new_primary_key_ast;
    }
    /// Reinitialize primary key because primary key column types might have changed.
    data.initPrimaryKey();

    /// Columns sizes could be changed
    data.recalculateColumnSizes();

    if (primary_key_is_modified)
        data.loadDataParts(false);
}

// somehow duplicated with `storage_modifier` in alterInternal ...
void StorageMergeTree::modifyASTStorage(ASTStorage * storage_ast, const TiDB::TableInfo & table_info_)
{
    if (!storage_ast || !storage_ast->engine || !storage_ast->engine->arguments)
        return;
    auto literal = std::make_shared<ASTLiteral>(Field(table_info_.serialize()));
    typeid_cast<ASTExpressionList &>(*storage_ast->engine->arguments).children.at(2) = literal;
}

/// While exists, marks parts as 'currently_merging' and reserves free space on filesystem.
/// It's possible to mark parts before.
struct CurrentlyMergingPartsTagger
{
    MergeTreeData::DataPartsVector parts;
    DiskSpaceMonitor::ReservationPtr reserved_space;
    StorageMergeTree * storage = nullptr;

    CurrentlyMergingPartsTagger() = default;

    CurrentlyMergingPartsTagger(const MergeTreeDataMerger::FuturePart & future_part, size_t total_size, StorageMergeTree & storage_)
        : parts(future_part.parts), storage(&storage_)
    {
        /// Assume mutex is already locked, because this method is called from mergeTask.
        reserved_space = DiskSpaceMonitor::reserve(future_part.path, total_size); /// May throw.
        for (const auto & part : parts)
        {
            if (storage->currently_merging.count(part))
                throw Exception("Tagging alreagy tagged part " + part->name + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);
        }
        storage->currently_merging.insert(parts.begin(), parts.end());
    }

    ~CurrentlyMergingPartsTagger()
    {
        std::lock_guard<std::mutex> lock(storage->currently_merging_mutex);

        for (const auto & part : parts)
        {
            if (!storage->currently_merging.count(part))
                std::terminate();
            storage->currently_merging.erase(part);
        }
    }
};


bool StorageMergeTree::merge(
    size_t aio_threshold,
    bool aggressive,
    const String & partition_id,
    bool final,
    bool deduplicate,
    String * out_disable_reason)
{
    /// Clear old parts. It does not matter to do it more frequently than each second.
    if (auto lock = time_after_previous_cleanup.compareAndRestartDeferred(1))
    {
        data.clearOldPartsFromFilesystem();
        data.clearOldTemporaryDirectories();
    }

    auto structure_lock = lockStructureForShare(context.getCurrentQueryId());

    size_t disk_space = DiskSpaceMonitor::getUnreservedFreeSpace(full_path);
    for (const auto & path : context.getPartPathSelector().getAllPath())
    {
        size_t available_space = DiskSpaceMonitor::getUnreservedFreeSpace(path);
        if (available_space <= disk_space)
        {
            disk_space = available_space;
        }
    }

    MergeTreeDataMerger::FuturePart future_part;

    /// You must call destructor with unlocked `currently_merging_mutex`.
    std::optional<CurrentlyMergingPartsTagger> merging_tagger;

    {
        std::lock_guard<std::mutex> lock(currently_merging_mutex);

        auto can_merge = [this] (const MergeTreeData::DataPartPtr & left, const MergeTreeData::DataPartPtr & right, String *)
        {
            return !currently_merging.count(left) && !currently_merging.count(right);
        };

        bool selected = false;

        if (partition_id.empty())
        {
            size_t max_parts_size_for_merge = merger.getMaxPartsSizeForMerge();
            if (max_parts_size_for_merge > 0)
                selected = merger.selectPartsToMerge(future_part, aggressive, max_parts_size_for_merge, can_merge, out_disable_reason);
        }
        else
        {
            selected = merger.selectAllPartsToMergeWithinPartition(future_part, disk_space, can_merge, partition_id, final, out_disable_reason);
        }

        if (!selected)
            return false;

        merging_tagger.emplace(future_part, MergeTreeDataMerger::estimateDiskSpaceForMerge(future_part.parts), *this);
    }

    MergeList::EntryPtr merge_entry = context.getMergeList().insert(database_name, table_name, future_part.name, future_part.parts);

    /// Logging
    Stopwatch stopwatch;
    MergeTreeData::MutableDataPartPtr new_part;

    auto write_part_log = [&] (const ExecutionStatus & execution_status)
    {
        try
        {
            auto part_log = context.getPartLog(database_name);
            if (!part_log)
                return;

            PartLogElement part_log_elem;

            part_log_elem.event_type = PartLogElement::MERGE_PARTS;
            part_log_elem.event_time = time(nullptr);
            part_log_elem.duration_ms = stopwatch.elapsed() / 1000000;

            part_log_elem.database_name = database_name;
            part_log_elem.table_name = table_name;
            part_log_elem.part_name = future_part.name;

            if (new_part)
                part_log_elem.bytes_compressed_on_disk = new_part->bytes_on_disk;

            part_log_elem.source_part_names.reserve(future_part.parts.size());
            for (const auto & source_part : future_part.parts)
                part_log_elem.source_part_names.push_back(source_part->name);

            part_log_elem.rows_read = (*merge_entry)->bytes_read_uncompressed;
            part_log_elem.bytes_read_uncompressed = (*merge_entry)->bytes_read_uncompressed;

            part_log_elem.rows = (*merge_entry)->rows_written;
            part_log_elem.bytes_uncompressed = (*merge_entry)->bytes_written_uncompressed;

            part_log_elem.error = static_cast<UInt16>(execution_status.code);
            part_log_elem.exception = execution_status.message;

            part_log->add(part_log_elem);
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
        }
    };

    try
    {
        new_part = merger.mergePartsToTemporaryPart(future_part, *merge_entry, aio_threshold, time(nullptr),
                                                    merging_tagger->reserved_space.get(), deduplicate, final);
        merger.renameMergedTemporaryPart(new_part, future_part.parts, nullptr);

        write_part_log({});
    }
    catch (...)
    {
        write_part_log(ExecutionStatus::fromCurrentException());
        throw;
    }

    return true;
}

bool StorageMergeTree::mergeTask()
{
    if (shutdown_called)
        return false;

    try
    {
        size_t aio_threshold = context.getSettings().min_bytes_to_use_direct_io;
        return merge(aio_threshold, false /*aggressive*/, {} /*partition_id*/, false /*final*/, false /*deduplicate*/); ///TODO: read deduplicate option from table config
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::ABORTED)
        {
            LOG_INFO(log, e.message());
            return false;
        }

        throw;
    }
}


void StorageMergeTree::clearColumnInPartition(const ASTPtr & partition, const Field & column_name, const Context & context)
{
    /// Asks to complete merges and does not allow them to start.
    /// This protects against "revival" of data for a removed partition after completion of merge.
    auto merge_blocker = merger.merges_blocker.cancel();

    /// We don't change table structure, only data in some parts, parts are locked inside alterDataPart() function
    auto lock_read_structure = lockStructureForShare(RWLock::NO_QUERY);

    String partition_id = data.getPartitionIDFromQuery(partition, context);
    MergeTreeData::DataParts parts = data.getDataParts();

    std::vector<MergeTreeData::AlterDataPartTransactionPtr> transactions;

    AlterCommand alter_command;
    alter_command.type = AlterCommand::DROP_COLUMN;
    alter_command.column_name = get<String>(column_name);

    auto new_columns = getColumns();
    alter_command.apply(new_columns);

    auto columns_for_parts = new_columns.getAllPhysical();
    for (const auto & part : parts)
    {
        if (part->info.partition_id != partition_id)
            continue;

        if (auto transaction = data.alterDataPart(part, columns_for_parts, data.primary_expr_ast, false))
            transactions.push_back(std::move(transaction));

        LOG_DEBUG(log, "Removing column " << get<String>(column_name) << " from part " << part->name);
    }

    if (transactions.empty())
        return;

    for (auto & transaction : transactions)
        transaction->commit();

    /// Recalculate columns size (not only for the modified column)
    data.recalculateColumnSizes();
}


bool StorageMergeTree::optimize(
    const ASTPtr & query, const ASTPtr & partition, bool final, bool deduplicate, const Context & context)
{
    std::ignore = query;

    String partition_id;
    if (partition)
        partition_id = data.getPartitionIDFromQuery(partition, context);

    String disable_reason;
    if (!merge(context.getSettingsRef().min_bytes_to_use_direct_io, true, partition_id, final, deduplicate, &disable_reason))
    {
        if (context.getSettingsRef().optimize_throw_if_noop)
            throw Exception(disable_reason.empty() ? "Can't OPTIMIZE by some reason" : disable_reason, ErrorCodes::CANNOT_ASSIGN_OPTIMIZE);
        return false;
    }

    return true;
}


void StorageMergeTree::dropPartition(const ASTPtr & /*query*/, const ASTPtr & partition, bool detach, const Context & context)
{
    /// Asks to complete merges and does not allow them to start.
    /// This protects against "revival" of data for a removed partition after completion of merge.
    auto merge_blocker = merger.merges_blocker.cancel();
    /// Waits for completion of merge and does not start new ones.
    auto lock = lockExclusively(context.getCurrentQueryId());

    String partition_id = data.getPartitionIDFromQuery(partition, context);

    size_t removed_parts = 0;
    MergeTreeData::DataParts parts = data.getDataParts();

    for (const auto & part : parts)
    {
        if (part->info.partition_id != partition_id)
            continue;

        LOG_DEBUG(log, "Removing part " << part->name);
        ++removed_parts;

        if (detach)
            data.renameAndDetachPart(part, "");
        else
            data.removePartsFromWorkingSet({part}, false);
    }

    LOG_INFO(log, (detach ? "Detached " : "Removed ") << removed_parts << " parts inside partition ID " << partition_id << ".");
}

void StorageMergeTree::truncate(const ASTPtr & /*query*/, const Context & /*context*/)
{
    auto lock = lockExclusively(context.getCurrentQueryId());

    MergeTreeData::DataParts parts = data.getDataParts();

    for (const auto & part : parts)
    {
        LOG_DEBUG(log, "Removing part " << part->name);
        data.removePartsFromWorkingSet({part}, true);
    }
}

void StorageMergeTree::attachPartition(const ASTPtr & partition, bool part, const Context & context)
{
    String partition_id;

    if (part)
        partition_id = typeid_cast<const ASTLiteral &>(*partition).value.safeGet<String>();
    else
        partition_id = data.getPartitionIDFromQuery(partition, context);

    String source_dir = "detached/";

    /// Let's make a list of parts to add.
    Strings parts;
    if (part)
    {
        parts.push_back(partition_id);
    }
    else
    {
        LOG_DEBUG(log, "Looking for parts for partition " << partition_id << " in " << source_dir);
        ActiveDataPartSet active_parts(data.format_version);
        for (Poco::DirectoryIterator it = Poco::DirectoryIterator(full_path + source_dir); it != Poco::DirectoryIterator(); ++it)
        {
            const String & name = it.name();
            MergeTreePartInfo part_info;
            if (!MergeTreePartInfo::tryParsePartName(name, &part_info, data.format_version)
                || part_info.partition_id != partition_id)
            {
                continue;
            }
            LOG_DEBUG(log, "Found part " << name);
            active_parts.add(name);
        }
        LOG_DEBUG(log, active_parts.size() << " of them are active");
        parts = active_parts.getParts();
    }

    for (const auto & source_part_name : parts)
    {
        String source_path = source_dir + source_part_name;

        LOG_DEBUG(log, "Checking data");
        MergeTreeData::MutableDataPartPtr part = data.loadPartAndFixMetadata(source_path);

        LOG_INFO(log, "Attaching part " << source_part_name << " from " << source_path);
        data.renameTempPartAndAdd(part, &increment);

        LOG_INFO(log, "Finished attaching part");
    }

    /// New parts with other data may appear in place of deleted parts.
    context.dropCaches();
}


void StorageMergeTree::freezePartition(const ASTPtr & partition, const String & with_name, const Context & context)
{
    data.freezePartition(partition, with_name, context);
}

DataTypePtr StorageMergeTree::getPKTypeImpl() const
{
    return data.primary_key_data_types[0];
}

}
