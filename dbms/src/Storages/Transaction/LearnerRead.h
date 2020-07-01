#include <Core/Types.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/Types.h>

#include <unordered_map>
#include <vector>


namespace DB
{

struct RegionLearnerReadSnapshot : RegionPtr
{
    UInt64 snapshot_event_flag{0};

    RegionLearnerReadSnapshot() = default;
    RegionLearnerReadSnapshot(const RegionPtr & region) : RegionPtr(region), snapshot_event_flag(region->getSnapshotEventFlag()) {}
    bool operator!=(const RegionPtr & tar) const { return (tar != *this) || (tar && snapshot_event_flag != tar->getSnapshotEventFlag()); }
};
using LearnerReadSnapshot = std::unordered_map<RegionID, RegionLearnerReadSnapshot>;

[[nodiscard]] LearnerReadSnapshot           //
doLearnerRead(const TiDB::TableID table_id, //
    const MvccQueryInfo & mvcc_query_info,  //
    TMTContext & tmt, Poco::Logger * log);

// After getting stream from storage, we should validate if regions have changed after learner read.
// If some region(s) changed, it will throw RegionException
void validateQueryInfo(const MvccQueryInfo::RegionsQueryInfo & regions_query_info, //
    const LearnerReadSnapshot & regions_snapshot, TMTContext & tmt, Poco::Logger * log);

} // namespace DB
