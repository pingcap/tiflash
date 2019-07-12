#include <Common/ClickHouseRevision.h>
#include <Common/config_version.h>

namespace ClickHouseRevision
{
    unsigned get() { return VERSION_REVISION; }
}

namespace TiFlashVersion
{
    std::string get() { return TIFLASH_VERSION_FULL; }
}
