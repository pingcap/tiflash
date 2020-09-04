#include <Common/config_version.h>

#include <ostream>
#include <string>

namespace TiFlashBuildInfo
{
std::string getName() { return TIFLASH_NAME; }
std::string getVersion() { return TIFLASH_VERSION; }
std::string getReleaseVersion() { return TIFLASH_RELEASE_VERSION; }
std::string getEdition() { return TIFLASH_EDITION; }
std::string getGitHash() { return TIFLASH_GIT_HASH; }
std::string getGitBranch() { return TIFLASH_GIT_BRANCH; }
std::string getUTCBuildTime() { return TIFLASH_UTC_BUILD_TIME; }
std::string getProfile() { return TIFLASH_PROFILE; }

void outputDetail(std::ostream & os)
{
    os << getName() << std::endl
       << "Release Version: " << getReleaseVersion() << std::endl
       << "Edition:         " << getEdition() << std::endl
       << "Git Commit Hash: " << getGitHash() << std::endl
       << "Git Branch:      " << getGitBranch() << std::endl
       << "UTC Build Time:  " << getUTCBuildTime() << std::endl
       << "Profile:         " << getProfile() << std::endl;
}
} // namespace TiFlashBuildInfo
