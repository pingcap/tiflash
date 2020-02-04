#pragma once

#include <ostream>
#include <string>

namespace TiFlashBuildInfo
{
std::string getName();
std::string getVersion();
std::string getGitTag();
std::string getGitHash();
std::string getUTCBuildTime();

void outputDetail(std::ostream & os);
} // namespace TiFlashBuildInfo
