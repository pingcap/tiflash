#include <Common/TiFlashMetrics.h>
#include <Interpreters/Context.h>
#include <Poco/File.h>
#include <RaftStoreProxyFFI/ColumnFamily.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
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
extern const int REGION_DATA_SCHEMA_UPDATED;
} // namespace ErrorCodes

std::tuple<Block, std::shared_ptr<StorageDeltaMerge>, DM::ColumnDefinesPtr> //
GenRegionBlockDatawithSchema(const RegionPtr & region, TMTContext & tmt);

bool atomicGetStorageIsCommonHandle(const RegionPtr & region, TMTContext & tmt);

namespace DM
{

SSTFilesToBlockInputStream::SSTFilesToBlockInputStream( //
    RegionPtr                      region_,
    const SSTViewVec &             snaps_,
    const TiFlashRaftProxyHelper * proxy_helper_,
    TMTContext &                   tmt_,
    size_t                         expected_size_)
    : region(std::move(region_)),
      snaps(snaps_),
      proxy_helper(proxy_helper_),
      tmt(tmt_),
      expected_size(expected_size_),
      log(&Poco::Logger::get("SSTFilesToBlockInputStream"))
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
}

void SSTFilesToBlockInputStream::readSuffix()
{
    // There must be no data left when we write suffix
    assert(!write_reader || !write_reader->remained());
    assert(!default_reader || !default_reader->remained());
    assert(!lock_reader || !lock_reader->remained());

    // reset all SSTReaders and return without writting blocks any more.
    write_reader.reset();
    default_reader.reset();
    lock_reader.reset();
}

Block SSTFilesToBlockInputStream::read()
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

bool SSTFilesToBlockInputStream::needUpdateSchema(const ColumnDefinesPtr & a, const ColumnDefinesPtr & b)
{
    // Note that we consider `a` is not `b` if both of them are `nullptr`
    if (a == nullptr || b == nullptr)
        return true;

    // If the two schema is not the same, then it need to be updated.
    if (a->size() != b->size())
        return true;
    for (size_t i = 0; i < a->size(); ++i)
    {
        const auto & ca = (*a)[i];
        const auto & cb = (*b)[i];

        bool col_ok = ca.id == cb.id;
        // bool name_ok = ca.name == cb.name;
        bool type_ok = ca.type->equals(*cb.type);

        if (!col_ok || !type_ok)
            return true;
    }
    return false;
}


Block SSTFilesToBlockInputStream::readCommitedBlock()
{
    if (is_decode_cancelled)
        return {};

    // Read block from `region`. If the schema has been updated, we need to
    // generate a new DTFile to store blocks with new schema.
    Block                              block;
    std::shared_ptr<StorageDeltaMerge> storage;
    ColumnDefinesPtr                   schema_snap;
    try
    {
        std::tie(block, storage, schema_snap) = GenRegionBlockDatawithSchema(region, tmt);
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

    // No committed rows.
    if (block.rows() == 0)
        return {};

    if (cur_schema && needUpdateSchema(cur_schema, schema_snap))
        throw Exception("", ErrorCodes::REGION_DATA_SCHEMA_UPDATED);

    ingest_storage = storage;
    cur_schema     = schema_snap;
    return block;
}

/// Methods for BoundedSSTFilesToBlockInputStream

BoundedSSTFilesToBlockInputStream::BoundedSSTFilesToBlockInputStream(SSTFilesToBlockInputStreamPtr child, ColId pk_column_id)
    : ReorganizeBlockInputStream(child, pk_column_id, false)
{
}

void BoundedSSTFilesToBlockInputStream::readPrefix()
{
    // Need to update `is_common_handle`
    auto * stream    = getChildStream();
    is_common_handle = atomicGetStorageIsCommonHandle(stream->region, stream->tmt);
    ReorganizeBlockInputStream::readPrefix();
}

const SSTFilesToBlockInputStream * BoundedSSTFilesToBlockInputStream::getChildStream() const
{
    SSTFilesToBlockInputStream * stream = nullptr;
    {
        std::shared_lock lock(children_mutex);
        stream = typeid_cast<SSTFilesToBlockInputStream *>(children[0].get());
    }
    if (likely(stream))
        return stream;
    throw Exception("Can not get SSTFilesToBlockInputStream, should not happen");
}

std::tuple<std::shared_ptr<StorageDeltaMerge>, DM::ColumnDefinesPtr> BoundedSSTFilesToBlockInputStream::ingestingInfo() const
{
    auto * stream = getChildStream();
    return std::make_tuple(stream->ingest_storage, stream->cur_schema);
}

size_t BoundedSSTFilesToBlockInputStream::getProcessKeys() const
{
    auto * stream = getChildStream();
    return stream->process_keys;
}

const RegionPtr BoundedSSTFilesToBlockInputStream::getRegion() const
{
    auto * stream = getChildStream();
    return stream->region;
}

Block BoundedSSTFilesToBlockInputStream::getHeader() const
{
    auto * stream = getChildStream();
    return stream->getHeader();
}

} // namespace DM
} // namespace DB
