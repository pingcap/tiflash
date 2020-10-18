#include <Flash/Coprocessor/DAGQueryBlockInterpreter.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/TiFlashSnapshotHandler.h>
#include <Interpreters/SQLQuerySource.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/IManageableStorage.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/TMTContext.h>

#include <iostream>

namespace DB
{

struct PreHandledTiFlashSnapshot
{
    ~PreHandledTiFlashSnapshot();
    RegionPtr region;
    std::string path;
};

PreHandledTiFlashSnapshot::~PreHandledTiFlashSnapshot()
{
    std::cerr << "GC PreHandledTiFlashSnapshot success"
              << "\n";
}

struct TiFlashSnapshot
{
    TiFlashSnapshot(const DM::ColumnDefines & write_columns_) : write_columns{write_columns_} {}
    Pipeline pipeline;
    const DM::ColumnDefines & write_columns;
    ~TiFlashSnapshot();
};

TiFlashSnapshot::~TiFlashSnapshot()
{
    std::cerr << "GC TiFlashSnapshot success"
              << "\n";
}

PreHandledTiFlashSnapshot * TiFlashSnapshotHandler::preHandleTiFlashSnapshot(RegionPtr region, const String & path)
{
    return new PreHandledTiFlashSnapshot{std::move(region), path};
}

void TiFlashSnapshotHandler::applyPreHandledTiFlashSnapshot(TMTContext * tmt, PreHandledTiFlashSnapshot * snap)
{
    std::cerr << "applyPreHandledTiFlashSnapshot: " << snap->region->toString() << "\n";
    auto & kvstore = tmt->getKVStore();
    kvstore->handleApplySnapshot(snap->region, *tmt);

    // TODO: check storage is not nullptr
    auto table_id = snap->region->getMappedTableID();
    auto storage = tmt->getStorages().get(table_id);
    auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);

    auto snapshot_file = DM::DMFile::restore(tmt->getContext().getFileProvider(), snap->path);
    auto column_cache = std::make_shared<DM::ColumnCache>();
    DM::DMFileBlockInputStream stream(tmt->getContext(),
        DM::MAX_UINT64,
        false,
        0,
        snapshot_file,
        dm_storage->getStore()->getTableColumns(),
        DM::RowKeyRange::newAll(dm_storage->isCommonHandle(), 1),
        DM::EMPTY_FILTER,
        column_cache,
        DM::IdSetPtr{});

    auto settings = tmt->getContext().getSettingsRef();
    stream.readPrefix();
    while (auto block = stream.read())
        dm_storage->write(std::move(block), settings);

    stream.readSuffix();
}

TiFlashSnapshot * TiFlashSnapshotHandler::genTiFlashSnapshot(TMTContext * tmt, uint64_t region_id)
{
    auto & kvstore = tmt->getKVStore();
    // generate snapshot struct;
    const RegionPtr region = kvstore->getRegion(region_id);
    auto region_range = region->getRange();
    auto table_id = region->getMappedTableID();
    auto storage = tmt->getStorages().get(table_id);
    Logger * log = &Logger::get("TiFlashSnapshotHandler");
    if (storage == nullptr)
    {
        LOG_WARNING(log,
            "genTiFlashSnapshot can not get table for region:" + region->toString() + " with table id: " + DB::toString(table_id)
                + ", ignored");
        return new TiFlashSnapshot(DM::ColumnDefines{});
    }

    auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);

    auto * snapshot = new TiFlashSnapshot(dm_storage->getStore()->getTableColumns());
    const Settings & settings = tmt->getContext().getSettingsRef();

    SelectQueryInfo query_info;
    // query_info.query is just a placeholder
    String query_str = "SELECT 1";
    SQLQuerySource query_src(query_str.data(), query_str.data() + query_str.size());
    std::tie(std::ignore, query_info.query) = query_src.parse(0);
    const ASTSelectWithUnionQuery & ast = typeid_cast<const ASTSelectWithUnionQuery &>(*query_info.query);
    query_info.query = ast.list_of_selects->children[0];

    auto mvcc_query_info = std::make_unique<MvccQueryInfo>();
    mvcc_query_info->resolve_locks = true;
    mvcc_query_info->read_tso = settings.read_tso;
    RegionQueryInfo info;
    {
        info.region_id = region_id;
        info.version = region->version();
        info.conf_version = region->confVer();
        info.range_in_table = region_range->rawKeys();
    }
    mvcc_query_info->regions_query_info.emplace_back(std::move(info));
    query_info.mvcc_query_info = std::move(mvcc_query_info);

    DAGPreparedSets dag_sets{};
    query_info.dag_query = std::make_unique<DAGQueryInfo>(std::vector<const tipb::Expr *>{}, dag_sets, std::vector<NameAndTypePair>{});

    QueryProcessingStage::Enum from_stage = QueryProcessingStage::FetchColumns;
    auto table_lock = storage->lockStructure(false, __PRETTY_FUNCTION__);
    Names required_columns = storage->getColumns().getNamesOfPhysical();
    snapshot->pipeline.streams = storage->read(required_columns, query_info, tmt->getContext(), from_stage, settings.max_block_size, 1);
    snapshot->pipeline.transform([&](auto & stream) { stream->addTableLock(table_lock); });
    return snapshot;
}

SerializeTiFlashSnapshotRes TiFlashSnapshotHandler::serializeTiFlashSnapshotInto(
    TMTContext * tmt, TiFlashSnapshot * snapshot, const String & path)
{
    if (snapshot->write_columns.empty())
        return {0, 0, 0};
    auto snapshot_file = DM::DMFile::create(path);
    uint64_t key_count = 0;
    DM::DMFileBlockOutputStream dst_stream(tmt->getContext(), snapshot_file, snapshot->write_columns);
    auto & src_stream = snapshot->pipeline.firstStream();
    src_stream->readPrefix();
    dst_stream.writePrefix();
    while (auto block = src_stream->read())
    {
        key_count += block.rows();
        dst_stream.write(block, 0);
    }
    src_stream->readSuffix();
    dst_stream.writeSuffix();
    Poco::File file(path);
    uint64_t total_size = file.getSize();
    // if key_count is 0, file will be deleted
    return {1, key_count, total_size};
}

bool TiFlashSnapshotHandler::isTiFlashSnapshot(TMTContext * tmt, const String & path)
{
    return DM::DMFile::isValidDMFileInSingleFileMode(tmt->getContext().getFileProvider(), path);
}

void TiFlashSnapshotHandler::deleteTiFlashSnapshot(TiFlashSnapshot * snap) { delete snap; }

void TiFlashSnapshotHandler::deletePreHandledTiFlashSnapshot(PreHandledTiFlashSnapshot * snap) { delete snap; }

} // namespace DB
