#include <Common/TiFlashMetrics.h>
#include <Interpreters/Context.h>
#include <Poco/File.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/SSTFilesToDTFilesOutputStream.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/SSTReader.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/logger_useful.h>

namespace DB
{


std::tuple<Block, std::shared_ptr<StorageDeltaMerge>, DM::ColumnDefinesPtr> //
GenRegionBlockDatawithSchema(const RegionPtr & region, TMTContext & tmt);

namespace DM
{

SSTFilesToDTFilesOutputStream::SSTFilesToDTFilesOutputStream( //
    RegionPtr                      region_,
    const SSTViewVec &             snaps_,
    uint64_t                       index_,
    uint64_t                       term_,
    TiDB::SnapshotApplyMethod      method_,
    const TiFlashRaftProxyHelper * proxy_helper_,
    TMTContext &                   tmt_,
    size_t                         expected_size_)
    : region(std::move(region_)),
      snaps(snaps_),
      snap_index(index_),
      snap_term(term_),
      method(method_),
      proxy_helper(proxy_helper_),
      tmt(tmt_),
      expected_size(expected_size_),
      log(&Poco::Logger::get("SSTFilesToDTFilesOutputStream"))
{
}

SSTFilesToDTFilesOutputStream::~SSTFilesToDTFilesOutputStream() = default;

void SSTFilesToDTFilesOutputStream::writePrefix()
{
    for (UInt64 i = 0; i < snaps.len; ++i)
    {
        auto & snapshot = snaps.views[i];
        switch (snapshot.type)
        {
        case ColumnFamilyType::Default:
            default_reader = std::make_unique<SSTReader>(proxy_helper, snapshot);
            break;
        case ColumnFamilyType::Write:
            write_reader = std::make_unique<SSTReader>(proxy_helper, snapshot);
            break;
        case ColumnFamilyType::Lock:
            lock_reader = std::make_unique<SSTReader>(proxy_helper, snapshot);
            break;
        }
    }

    process_keys   = 0;
    process_writes = 0;
    watch.start();
}

void SSTFilesToDTFilesOutputStream::writeSuffix()
{
    // There must be no data left when we write suffix
    assert(!write_reader || !write_reader->remained());
    assert(!default_reader || !default_reader->remained());
    assert(!lock_reader || !lock_reader->remained());

    finishCurrDTFileStream();

    auto & ctx     = tmt.getContext();
    auto   metrics = ctx.getTiFlashMetrics();
    GET_METRIC(metrics, tiflash_raft_command_duration_seconds, type_apply_snapshot_predecode).Observe(watch.elapsedSeconds());
    LOG_INFO(log,
             "Pre-handle snapshot " << region->toString(false) << " to " << ingest_file_ids.size() << " DTFiles, cost "
                                    << watch.elapsedMilliseconds() << "ms [rows=" << process_writes << "]");
    // Note that number of keys in different cf will be aggregated into one metrics
    GET_METRIC(metrics, tiflash_raft_process_keys, type_apply_snapshot).Increment(process_keys);
}

void SSTFilesToDTFilesOutputStream::write()
{
    while (write_reader && write_reader->remained())
    {
        // Read a key-value from write CF and continue to read key-value until
        // the key that we read from write CF.
        // All SST files store key-value in a sorted way so that we are able to
        // scan committed rows in `region`.
        auto key   = write_reader->key();
        auto value = write_reader->value();
        region->insert(ColumnFamilyType::Write, TiKVKey(key.data, key.len), TiKVValue(value.data, value.len));
        ++process_keys;

        auto key_view = std::string_view{key.data, key.len};
        scanCF(ColumnFamilyType::Default, /*until*/ key_view);
        scanCF(ColumnFamilyType::Lock, /*until*/ key_view);

        ++process_writes;
        write_reader->next();

        if (process_writes % expected_size == 0)
        {
            saveCommitedData();
        }
    }
    // Scan all key-value pair from other CFs
    scanCF(ColumnFamilyType::Default);
    scanCF(ColumnFamilyType::Lock);

    saveCommitedData();

    // All uncommitted data are saved in `region`.
}

void SSTFilesToDTFilesOutputStream::scanCF(ColumnFamilyType cf, const std::string_view until)
{
    SSTReader * reader;
    if (cf == ColumnFamilyType::Default)
        reader = default_reader.get();
    else if (cf == ColumnFamilyType::Lock)
        reader = lock_reader.get();
    else
        throw Exception("Should not happen!");

    while (reader && reader->remained())
    {
        auto key = reader->key();
        if (until.data() == nullptr || memcmp(until.data(), key.data, std::min(until.size(), key.len)) <= 0)
        {
            auto value = reader->value();
            region->insert(cf, TiKVKey(key.data, key.len), TiKVValue(value.data, value.len));
            ++process_keys;
            reader->next();
        }
        else
            break;
    }
}

bool needUpdateSchema(const ColumnDefinesPtr & a, const ColumnDefinesPtr & b)
{
    // Note that we consider `a` is not `b` if both of them are `nullptr`
    if (a == nullptr || b == nullptr)
        return false;

    if (a->size() != b->size())
        return false;
    for (size_t i = 0; i < a->size(); ++i)
    {
        const auto & ca = (*a)[i];
        const auto & cb = (*b)[i];

        bool col_ok = ca.id == cb.id;
        // bool name_ok = ca.name == cb.name;
        bool type_ok = ca.type->equals(*cb.type);

        if (!col_ok || !type_ok)
            return false;
    }
    return true;
}

void SSTFilesToDTFilesOutputStream::saveCommitedData()
{
    // Read block from `region`. If the schema has been updated, we need to
    // generate a new DTFile to store blocks with new schema.
    Block                              block;
    std::shared_ptr<StorageDeltaMerge> storage;
    ColumnDefinesPtr                   schema_snap;
    std::tie(block, storage, schema_snap) = GenRegionBlockDatawithSchema(region, tmt);

    if (block.rows() == 0)
        return;

    ingest_storage = storage;
    if (dt_file == nullptr || !needUpdateSchema(cur_schema, schema_snap))
    {
        // Close previous DTFile and output stream before creating new DTFile for new schema
        finishCurrDTFileStream();

        // Generate a DMFilePtr and its DMFileBlockOutputStream if not exists
        bool single_file_mode = /*!write_reader || !write_reader->remained();*/ false;
        switch (method)
        {
        case TiDB::SnapshotApplyMethod::DTFile_Directory:
            single_file_mode = false;
            break;
        case TiDB::SnapshotApplyMethod::DTFile_Single:
            single_file_mode = true;
            break;
        default:
            break;
        }

        // The parent_path and file_id are generated by storage.
        auto [parent_path, file_id] = ingest_storage->getStore()->preAllocateIngestFile();
        if (parent_path.empty())
        {
            // Can no allocate path and id for storing DTFiles (the store may be dropped),
            // reset all SSTReaders and return without writting blocks any more.
            write_reader.reset();
            default_reader.reset();
            lock_reader.reset();
            return;
        }
        dt_file = DMFile::create(file_id, parent_path, single_file_mode);
        LOG_INFO(log, "Create file for snapshot data [file=" << dt_file->path() << "] [single_file_mode=" << single_file_mode << "]");
        cur_schema = schema_snap;
        dt_stream  = std::make_unique<DMFileBlockOutputStream>(tmt.getContext(), dt_file, *cur_schema, /*need_rate_limit=*/false);
        dt_stream->writePrefix();
        ingest_file_ids.emplace_back(file_id);
    }

    // Write block to the output stream
    dt_stream->write(block, /*not_clean_rows=*/1);
}

void SSTFilesToDTFilesOutputStream::finishCurrDTFileStream()
{
    if (dt_file == nullptr)
        return;

    assert(dt_file != nullptr);
    assert(dt_stream != nullptr);

    dt_stream->writeSuffix();
    dt_stream.reset();
    // The DTFile should not be able to gc until it is ingested.
    assert(!dt_file->canGC());
    // Add the DTFile to StoragePathPool so that we can restore it later
    ingest_storage->getStore()->preIngestFile(dt_file->parentPath(), dt_file->fileId(), dt_file->getBytesOnDisk());
    dt_file.reset();
}


} // namespace DM
} // namespace DB
