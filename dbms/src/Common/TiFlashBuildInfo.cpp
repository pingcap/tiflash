#include <Common/config_version.h>

#include <ostream>
#include <string>

namespace TiFlashBuildInfo
{
std::string getName() { return TIFLASH_NAME; }
std::string getVersion() { return TIFLASH_VERSION; }
std::string getGitTag() { return TIFLASH_GITTAG; }
std::string getGitHash() { return TIFLASH_GITHASH; }
std::string getUTCBuildTime() { return TIFLASH_UTC_BUILD_TIME; }

void outputDetail(std::ostream & os)
{
    os << getName() << std::endl
       << "Version:        " << getVersion() << std::endl
       << "Git Tag:        " << getGitTag() << std::endl
       << "Git Hash:       " << getGitHash() << std::endl
       << "UTC Build Time: " << getUTCBuildTime() << std::endl;
}
} // namespace TiFlashBuildInfo
