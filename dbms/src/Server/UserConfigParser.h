#include <Core/Types.h>

#include <memory>

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
class Context;
class ConfigReloader;
using ConfigReloaderPtr = std::unique_ptr<ConfigReloader>;
namespace UserConfig
{

ConfigReloaderPtr parseSettings(Poco::Util::LayeredConfiguration & config, const std::string & config_path,
    std::unique_ptr<Context> & global_context, Poco::Logger * log);

}
} // namespace DB
