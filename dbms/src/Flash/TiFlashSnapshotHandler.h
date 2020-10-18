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

class TiFlashSnapshotHandler
{
public:
    static PreHandledTiFlashSnapshot * preHandleTiFlashSnapshot(RegionPtr region, const String & path);

    static void applyPreHandledTiFlashSnapshot(TMTContext * tmt, PreHandledTiFlashSnapshot * snap);

    static TiFlashSnapshot * genTiFlashSnapshot(TMTContext * tmt, uint64_t region_id);

    static SerializeTiFlashSnapshotRes serializeTiFlashSnapshotInto(TMTContext * tmt, TiFlashSnapshot * snapshot, const String & path);

    static bool isTiFlashSnapshot(TMTContext * tmt, const String & path);

    static void deleteTiFlashSnapshot(TiFlashSnapshot * snap);

    static void deletePreHandledTiFlashSnapshot(PreHandledTiFlashSnapshot * snap);
};

} // namespace DB
