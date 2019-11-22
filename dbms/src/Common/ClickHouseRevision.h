#pragma once

#include <string>


namespace ClickHouseRevision
{
    unsigned get();
}

namespace TiFlashVersion
{
    std::string get();
}

namespace TiFlashBuildInfo
{
    std::string getGitBranch();
    std::string getGitHash();
    std::string getVersionString();
    std::string getUTCBuildTime();
}

