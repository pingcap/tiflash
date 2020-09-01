#pragma once

#include <ostream>
#include <string>

namespace TiFlashBuildInfo
{
std::string getName();
/// Semantic version.
std::string getVersion();
/// Release version that follows PD/TiKV/TiDB convention.
std::string getReleaseVersion();
std::string getEdition();
std::string getGitHash();
std::string getGitBranch();
std::string getUTCBuildTime();
std::string getBuildType();

void outputDetail(std::ostream & os);
} // namespace TiFlashBuildInfo
