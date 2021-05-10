#include <Core/Types.h>
#include <Storages/Transaction/StorageEngineType.h>

#include <unordered_set>

namespace Poco
{
class Logger;
namespace Util
{
class LayeredConfiguration;
}
} // namespace Poco

namespace DB
{

struct TiFlashRaftConfig
{
    const std::string engine_key = "engine";
    const std::string engine_value = "tiflash";
    Strings pd_addrs;
    std::unordered_set<std::string> ignore_databases{"system"};
    // Actually it is "flash.service_addr"
    std::string flash_server_addr;
    bool enable_compatible_mode = true;

    static constexpr TiDB::StorageEngine DEFAULT_ENGINE = TiDB::StorageEngine::DT;
    bool disable_bg_flush = false;
    TiDB::StorageEngine engine = DEFAULT_ENGINE;
    TiDB::SnapshotApplyMethod snapshot_apply_method = TiDB::SnapshotApplyMethod::DTFile_Directory;

public:
    TiFlashRaftConfig() = default;

    static TiFlashRaftConfig parseSettings(Poco::Util::LayeredConfiguration & config, Poco::Logger * log);

};

} // namespace DB
