#include <Storages/Transaction/Region.h>

#include <cstdint>
#include <string>

namespace DB
{
class TMTContext;

struct PreHandledTiFlashSnapshot;
struct TiFlashSnapshot;

struct SerializeTiFlashSnapshotRes
{
    uint8_t ok;
    uint64_t key_count;
    uint64_t total_size;
};
using String = std::string;

PreHandledTiFlashSnapshot *preHandleTiFlashSnapshot(RegionPtr region, const String & path);
void applyPreHandledTiFlashSnapshot(TMTContext * tmt, PreHandledTiFlashSnapshot * snap);
TiFlashSnapshot *genTiFlashSnapshot(TMTContext * tmt, uint64_t region_id);
SerializeTiFlashSnapshotRes serializeTiFlashSnapshotInto(TMTContext * tmt, TiFlashSnapshot *snapshot, const String & path);
bool isTiFlashSnapshot(TMTContext * tmt, const String & path);
void deleteTiFlashSnapshot(TiFlashSnapshot * snap);
void deletePreHandledTiFlashSnapshot(PreHandledTiFlashSnapshot * snap);

}

