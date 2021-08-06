#include <Core/Types.h>

#include <tuple>
#include <vector>

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

struct TiFlashStorageConfig
{
public:
    Strings main_data_paths;
    std::vector<size_t> main_capacity_quota;
    Strings latest_data_paths;
    std::vector<size_t> latest_capacity_quota;
    Strings kvstore_data_path;

    UInt64 bg_task_io_rate_limit = 0;
    UInt64 format_version = 0;
    bool lazily_init_store = true;
public:
    TiFlashStorageConfig() {}

    Strings getAllNormalPaths() const;

    static std::tuple<size_t, TiFlashStorageConfig> parseSettings(Poco::Util::LayeredConfiguration & config, Poco::Logger * log);

private:
    void parse(const String & storage_section, Poco::Logger * log);

    bool parseFromDeprecatedConfiguration(Poco::Util::LayeredConfiguration & config, Poco::Logger * log);
};


} // namespace DB
