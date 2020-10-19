#include <Flash/Coprocessor/DAGQueryBlockInterpreter.h>
#include <Flash/TiFlashSnapshotHandler.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/TMTContext.h>

#include <iostream>

namespace DB
{

struct PreHandledTiFlashSnapshot
{
    RegionPtr region;
    std::string path;
    ~PreHandledTiFlashSnapshot();
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

    auto table_id = snap->region->getMappedTableID();
    auto storage = tmt->getStorages().get(table_id);
    Logger * log = &Logger::get("TiFlashSnapshotHandler");
    if (storage == nullptr)
    {
        LOG_WARNING(log,
            "applyPreHandledTiFlashSnapshot can not get table for region:" + snap->region->toString()
                + " with table id: " + DB::toString(table_id) + ", ignored");
        return;
    }
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

    const Settings & settings = tmt->getContext().getSettingsRef();
    auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
    auto dm_store = dm_storage->getStore();
    auto columns_to_read = dm_store->getTableColumns();
    auto * snapshot = new TiFlashSnapshot(columns_to_read);
    auto range
        = DM::RowKeyRange::fromRegionRange(region->getRange(), table_id, dm_store->isCommonHandle(), dm_store->getRowKeyColumnSize());

    auto table_lock = storage->lockStructure(false, __PRETTY_FUNCTION__);
    snapshot->pipeline.streams = dm_store->read(
        tmt->getContext(), settings, columns_to_read, {range}, 1, DM::MAX_UINT64, DM::EMPTY_FILTER, DEFAULT_BLOCK_SIZE, {}, true);
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
