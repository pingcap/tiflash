#include <Common/TiFlashMetrics.h>
#include <Interpreters/Context.h>
#include <Poco/File.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/PKSquashingBlockInputStream.h>
#include <Storages/DeltaMerge/SSTFilesToBlockInputStream.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/SSTReader.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLFORMAT_RAFT_ROW;
} // namespace ErrorCodes

Block GenRegionBlockDatawithSchema( //
    const RegionPtr &,
    const std::shared_ptr<StorageDeltaMerge> &,
    const DM::ColumnDefinesPtr &,
    Timestamp,
    bool,
    TMTContext &);

namespace DM
{

SSTFilesToBlockInputStream::SSTFilesToBlockInputStream( //
    RegionPtr                                        region_,
    const SSTViewVec &                               snaps_,
    const TiFlashRaftProxyHelper *                   proxy_helper_,
    SSTFilesToBlockInputStream::StorageDeltaMergePtr ingest_storage_,
    DM::ColumnDefinesPtr                             schema_snap_,
    Timestamp                                        gc_safepoint_,
    bool                                             force_decode_,
    TMTContext &                                     tmt_,
    size_t                                           expected_size_)
    : region(std::move(region_)),
      snaps(snaps_),
      proxy_helper(proxy_helper_),
      ingest_storage(std::move(ingest_storage_)),
      schema_snap(std::move(schema_snap_)),
      tmt(tmt_),
      gc_safepoint(gc_safepoint_),
      expected_size(expected_size_),
      log(&Poco::Logger::get("SSTFilesToBlockInputStream")),
      force_decode(force_decode_)
{
}

SSTFilesToBlockInputStream::~SSTFilesToBlockInputStream() = default;

void SSTFilesToBlockInputStream::readPrefix()
{
    for (UInt64 i = 0; i < snaps.len; ++i)
    {
        auto & snapshot = snaps.views[i];
        switch (snapshot.type)
        {
        case ColumnFamilyType::Default:
            default_cf_reader = std::make_unique<SSTReader>(proxy_helper, snapshot);
            break;
        case ColumnFamilyType::Write:
            write_cf_reader = std::make_unique<SSTReader>(proxy_helper, snapshot);
            break;
        case ColumnFamilyType::Lock:
            lock_cf_reader = std::make_unique<SSTReader>(proxy_helper, snapshot);
            break;
        }
    }

    process_keys.default_cf = 0;
    process_keys.write_cf   = 0;
    process_keys.lock_cf    = 0;
}

void SSTFilesToBlockInputStream::readSuffix()
{
    // There must be no data left when we write suffix
    assert(!write_cf_reader || !write_cf_reader->remained());
    assert(!default_cf_reader || !default_cf_reader->remained());
    assert(!lock_cf_reader || !lock_cf_reader->remained());

    // reset all SSTReaders and return without writting blocks any more.
    write_cf_reader.reset();
    default_cf_reader.reset();
    lock_cf_reader.reset();
}

Block SSTFilesToBlockInputStream::read()
{
    while (write_cf_reader && write_cf_reader->remained())
    {
        // Read a key-value from write CF and continue to read key-value until
        // the key that we read from write CF.
        // All SST files store key-value in a sorted way so that we are able to
        // scan committed rows in `region`.
        auto key   = write_cf_reader->key();
        auto value = write_cf_reader->value();
        region->insert(ColumnFamilyType::Write, TiKVKey(key.data, key.len), TiKVValue(value.data, value.len));
        ++process_keys.write_cf;
        write_cf_reader->next();

        if (process_keys.write_cf % expected_size == 0)
        {
            auto key_view = std::string_view{key.data, key.len};
            // Batch the scan from other CFs until we need to decode data
            scanCF(ColumnFamilyType::Default, /*until*/ key_view);
            scanCF(ColumnFamilyType::Lock, /*until*/ key_view);

            auto block = readCommitedBlock();
            if (block.rows() != 0)
                return block;
            // else continue to decode key-value from write CF.
        }
    }
    // Scan all key-value pairs from other CFs
    scanCF(ColumnFamilyType::Default);
    scanCF(ColumnFamilyType::Lock);

    // All uncommitted data are saved in `region`, decode the last committed rows.
    return readCommitedBlock();
}

void SSTFilesToBlockInputStream::scanCF(ColumnFamilyType cf, const std::string_view until)
{
    SSTReader * reader;
    if (cf == ColumnFamilyType::Default)
        reader = default_cf_reader.get();
    else if (cf == ColumnFamilyType::Lock)
        reader = lock_cf_reader.get();
    else
        throw Exception("Should not happen!");

    size_t num_process_keys = 0;
    while (reader && reader->remained())
    {
        auto key = reader->key();
        if (until.data() == nullptr || memcmp(until.data(), key.data, std::min(until.size(), key.len)) >= 0)
        {
            auto value = reader->value();
            region->insert(cf, TiKVKey(key.data, key.len), TiKVValue(value.data, value.len));
            ++num_process_keys;
            reader->next();
        }
        else
            break;
    }

    if (cf == ColumnFamilyType::Default)
        process_keys.default_cf += num_process_keys;
    else if (cf == ColumnFamilyType::Lock)
        process_keys.lock_cf += num_process_keys;
}

Block SSTFilesToBlockInputStream::readCommitedBlock()
{
    if (is_decode_cancelled)
        return {};

    try
    {
        // Read block from `region`. If the schema has been updated, it will
        // throw an exception with code `ErrorCodes::REGION_DATA_SCHEMA_UPDATED`
        return GenRegionBlockDatawithSchema(region, ingest_storage, schema_snap, gc_safepoint, force_decode, tmt);
    }
    catch (DB::Exception & e)
    {
        if (e.code() == ErrorCodes::ILLFORMAT_RAFT_ROW)
        {
            // br or lighting may write illegal data into tikv, stop decoding.
            LOG_WARNING(log,
                        "Got error while reading region committed cache: "
                            << e.displayText() << ". Stop decoding rows into DTFiles and keep uncommitted data in region.");
            // Cancel the decoding process.
            // Note that we still need to scan data from CFs and keep them in `region`
            is_decode_cancelled = true;
            return {};
        }
        else
            throw;
    }
}

/// Methods for BoundedSSTFilesToBlockInputStream

BoundedSSTFilesToBlockInputStream::BoundedSSTFilesToBlockInputStream( //
    SSTFilesToBlockInputStreamPtr child,
    const ColId                   pk_column_id_,
    const bool                    is_common_handle_)
    : pk_column_id(pk_column_id_), is_common_handle(is_common_handle_), _raw_child(std::move(child))
{
    // Initlize `mvcc_compact_stream`
    // First refine the boundary of blocks. Note that the rows decoded from SSTFiles are sorted by primary key asc, timestamp desc
    // (https://github.com/tikv/tikv/blob/v5.0.1/components/txn_types/src/types.rs#L103-L108).
    // While DMVersionFilter require rows sorted by primary key asc, timestamp asc, so we need an extra sort in PKSquashing.
    auto stream = std::make_shared<PKSquashingBlockInputStream</*need_extra_sort=*/true>>(_raw_child, pk_column_id, is_common_handle);
    mvcc_compact_stream = std::make_unique<DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>>(
        stream, *(_raw_child->schema_snap), _raw_child->gc_safepoint, is_common_handle);
}

void BoundedSSTFilesToBlockInputStream::readPrefix()
{
    mvcc_compact_stream->readPrefix();
}

void BoundedSSTFilesToBlockInputStream::readSuffix()
{
    mvcc_compact_stream->readSuffix();
}

Block BoundedSSTFilesToBlockInputStream::read()
{
    return mvcc_compact_stream->read();
}

std::tuple<std::shared_ptr<StorageDeltaMerge>, DM::ColumnDefinesPtr> BoundedSSTFilesToBlockInputStream::ingestingInfo() const
{
    return std::make_tuple(_raw_child->ingest_storage, _raw_child->schema_snap);
}

SSTFilesToBlockInputStream::ProcessKeys BoundedSSTFilesToBlockInputStream::getProcessKeys() const
{
    return _raw_child->process_keys;
}

const RegionPtr BoundedSSTFilesToBlockInputStream::getRegion() const
{
    return _raw_child->region;
}

std::tuple<size_t, size_t, UInt64> //
BoundedSSTFilesToBlockInputStream::getMvccStatistics() const
{
    return std::make_tuple(
        mvcc_compact_stream->getEffectiveNumRows(), mvcc_compact_stream->getNotCleanRows(), mvcc_compact_stream->getGCHintVersion());
}

} // namespace DM
} // namespace DB
