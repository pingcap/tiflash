#include <Flash/Coprocessor/DAGQueryBlockInterpreter.h>
#include <Flash/TiFlashSnapshotHandler.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/Segment.h>
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
    TiFlashSnapshot(DM::ColumnDefines write_columns_, BlockInputStreamPtr stream_)
        : write_columns{std::move(write_columns_)}, stream{std::move(stream_)}
    {}
    DM::ColumnDefines write_columns;
    BlockInputStreamPtr stream;
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
    auto columns_to_read = dm_storage->getStore()->getTableColumns();
    auto column_cache = std::make_shared<DM::ColumnCache>();
    DM::DMFileBlockInputStream stream(tmt->getContext(),
        DM::MAX_UINT64,
        false,
        0,
        snapshot_file,
        columns_to_read,
        DM::RowKeyRange::newAll(dm_storage->isCommonHandle(), dm_storage->getRowKeyColumnSize()),
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
        return new TiFlashSnapshot(DM::ColumnDefines{}, nullptr);
    }

    const Settings & settings = tmt->getContext().getSettingsRef();
    auto dm_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
    auto dm_store = dm_storage->getStore();

    auto range
        = DM::RowKeyRange::fromRegionRange(region->getRange(), table_id, dm_storage->isCommonHandle(), dm_store->getRowKeyColumnSize());
    auto streams = dm_store->read(tmt->getContext(),
        settings,
        *dm_store->getPhysicalColumns(),
        {range},
        1,
        DM::MAX_UINT64,
        DM::EMPTY_FILTER,
        DEFAULT_BLOCK_SIZE,
        {},
        true);
    auto * snapshot = new TiFlashSnapshot(*dm_store->getPhysicalColumns(), streams.at(0));
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
    auto & src_stream = snapshot->stream;
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
