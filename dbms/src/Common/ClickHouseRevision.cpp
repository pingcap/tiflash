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
namespace TiFlashBuildInfo
{
    std::string getGitBranch() { return TIFLASH_BRANCH; }
    std::string getGitHash() { return TIFLASH_GITHASH; }
    std::string getVersionString() { return TIFLASH_VERSION_STRING; }
    std::string getUTCBuildTime() { return TIFLASH_UTC_BUILD_TIME; }
}
