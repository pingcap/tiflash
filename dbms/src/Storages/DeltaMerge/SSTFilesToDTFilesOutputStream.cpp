#include <Common/TiFlashMetrics.h>
#include <Interpreters/Context.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/SSTFilesToDTFilesOutputStream.h>
#include <Storages/Transaction/ProxyFFIType.h>
#include <Storages/Transaction/RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/SSTReader.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/logger_useful.h>

namespace DB
{

extern RegionPtrWrap::CachePtr GenRegionPreDecodeBlockData(const RegionPtr &, Context &);

namespace DM
{
struct ColumnDefine;
using ColumnDefines    = std::vector<ColumnDefine>;
using ColumnDefinesPtr = std::shared_ptr<ColumnDefines>;
} // namespace DM
extern std::tuple<Block, bool, DM::ColumnDefinesPtr> GenRegionPreDecodeBlockDataNew(const RegionPtr & region, TMTContext & tmt);

namespace DM
{

SSTFilesToDTFilesOutputStream::SSTFilesToDTFilesOutputStream( //
    RegionPtr                      region_,
    const SSTViewVec &             snaps_,
    uint64_t                       index_,
    uint64_t                       term_,
    const TiFlashRaftProxyHelper * proxy_helper_,
    TMTContext &                   tmt_)
    : region(std::move(region_)),
      snaps(snaps_),
      snap_index(index_),
      snap_term(term_),
      proxy_helper(proxy_helper_),
      tmt(tmt_),
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
            default_reader = std::make_unique<SSTReader>(proxy_helper, snaps.views[i]);
            break;
        case ColumnFamilyType::Write:
            write_reader = std::make_unique<SSTReader>(proxy_helper, snaps.views[i]);
            break;
        case ColumnFamilyType::Lock:
            lock_reader = std::make_unique<SSTReader>(proxy_helper, snaps.views[i]);
            break;
        }
    }

    {
        // Create a directory with id `regionID_term_idx` for this snapshot
        snap_dir = tmt.getContext().getTemporaryPath() + "/snap_" + DB::toString(region->id()) + "_" + DB::toString(snap_term) + "_"
            + DB::toString(snap_index);
        auto f = Poco::File(snap_dir);
        if (f.exists())
            f.remove(true);
        f.createDirectories();
    }

    curr_file_id   = 0;
    process_keys   = 0;
    process_writes = 0;
    watch.start();
}

void SSTFilesToDTFilesOutputStream::writeSuffix()
{
    assert(!write_reader || !write_reader->remained());
    assert(!default_reader || !default_reader->remained());
    assert(!lock_reader || !lock_reader->remained());

    if (dt_stream)
        finishCurrDTFileStream();

    auto & ctx     = tmt.getContext();
    auto   metrics = ctx.getTiFlashMetrics();
    GET_METRIC(metrics, tiflash_raft_command_duration_seconds, type_apply_snapshot_predecode).Observe(watch.elapsedSeconds());
    LOG_INFO(log, "Pre-handle snapshot " << region->toString(false) << " cost " << watch.elapsedMilliseconds() << "ms");
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

        if (process_writes % 8192 == 0) // FIXME: magic number
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


void SSTFilesToDTFilesOutputStream::saveCommitedData()
{
    // Read block from `region`. If the schema has been updated, we need to
    // generate a new DTFile to store blocks with new schema.
    Block            block;
    bool             schema_sync_triggered = false;
    ColumnDefinesPtr schema_snap;
    std::tie(block, schema_sync_triggered, schema_snap) = GenRegionPreDecodeBlockDataNew(region, tmt);

    if (block.rows() == 0)
        return;

    if (dt_file == nullptr || schema_sync_triggered)
    {
        // Close previous DTFile and output stream before creating new DTFile for new schema
        if (dt_file != nullptr)
            finishCurrDTFileStream();
        // Generate a DMFilePtr and its DMFileBlockOutputStream if not exists
        // TODO: Add some logging
        bool single_file_mode = /*!write_reader || !write_reader->remained();*/ true;
        dt_file               = DMFile::create(/*file_id*/ ++curr_file_id, /*parent_path*/ snap_dir, single_file_mode);
        dt_stream = std::make_unique<DMFileBlockOutputStream>(tmt.getContext(), dt_file, *schema_snap, /*need_rate_limit=*/false);
        dt_stream->writePrefix();
    }

    // Write block to the output stream
    dt_stream->write(block, /*not_clean_rows=*/1);
}

void SSTFilesToDTFilesOutputStream::finishCurrDTFileStream()
{
    assert(dt_file != nullptr);
    assert(dt_stream != nullptr);

    dt_stream->writeSuffix();
    dt_stream.reset();
    dt_file.reset();
}


} // namespace DM
} // namespace DB
