
#include <RaftStoreProxyFFI/ProxyFFI.h>
#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>
#include <Storages/KVStore/FFI/SSTReader.h>
#include <Storages/KVStore/MultiRaft/RegionState.h>
#include <Storages/KVStore/TiKVHelpers/TiKVKeyValue.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>

namespace DB::DM
{

struct SSTScanSoftLimit
{
    constexpr static size_t HEAD_OR_ONLY_SPLIT = SIZE_MAX;
    size_t split_id;
    TiKVKey raw_start;
    TiKVKey raw_end;
    DecodedTiKVKey decoded_start;
    DecodedTiKVKey decoded_end;
    std::optional<RawTiDBPK> start_limit;
    std::optional<RawTiDBPK> end_limit;

    SSTScanSoftLimit(size_t split_id_, TiKVKey && raw_start_, TiKVKey && raw_end_)
        : split_id(split_id_)
        , raw_start(std::move(raw_start_))
        , raw_end(std::move(raw_end_))
    {
        if (!raw_start.empty())
        {
            decoded_start = RecordKVFormat::decodeTiKVKey(raw_start);
        }
        if (!raw_end.empty())
        {
            decoded_end = RecordKVFormat::decodeTiKVKey(raw_end);
        }
        if (!decoded_start.empty())
        {
            start_limit = RecordKVFormat::getRawTiDBPK(decoded_start);
        }
        if (!decoded_end.empty())
        {
            end_limit = RecordKVFormat::getRawTiDBPK(decoded_end);
        }
    }

    SSTScanSoftLimit clone() const { return SSTScanSoftLimit(split_id, raw_start.toString(), raw_end.toString()); }

    const std::optional<RawTiDBPK> & getStartLimit() const { return start_limit; }

    const std::optional<RawTiDBPK> & getEndLimit() const { return end_limit; }

    std::string toDebugString() const
    {
        return fmt::format("{}:{}", raw_start.toDebugString(), raw_end.toDebugString());
    }
};

struct SnapshotSSTReader;
using SnapshotSSTReaderPtr = std::shared_ptr<SnapshotSSTReader>;
struct SnapshotSSTReader
{
    SnapshotSSTReader(
        const SSTViewVec & snaps,
        const TiFlashRaftProxyHelper * proxy_helper,
        RegionID region_id,
        UInt64 snapshot_index,
        const ImutRegionRangePtr & region_range,
        std::optional<SSTScanSoftLimit> && soft_limit_,
        const String & log_prefix);

    // Currently it only takes effect if using tablet sst reader which is usually a raftstore v2 case.
    // Otherwise will return zero.
    size_t getApproxBytes() const;
    std::vector<std::string> findSplitKeys(size_t splits_count) const;
    size_t getSplitId() const
    {
        return soft_limit.has_value() ? soft_limit.value().split_id : DM::SSTScanSoftLimit::HEAD_OR_ONLY_SPLIT;
    }

    using SSTReaderPtr = std::unique_ptr<SSTReader>;
    bool maybeSkipBySoftLimit(ColumnFamilyType cf, SSTReaderPtr & reader);
    bool maybeStopBySoftLimit(ColumnFamilyType cf, SSTReaderPtr & reader);

    std::optional<SSTScanSoftLimit> soft_limit;
    LoggerPtr log;
    SSTReaderPtr write_cf_reader;
    SSTReaderPtr default_cf_reader;
    SSTReaderPtr lock_cf_reader;
};
} // namespace DB::DM
