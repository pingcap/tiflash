#pragma once

#include <string>

namespace TiDB
{
bool CheckMppVersion(int64_t mpp_version);
std::string GenMppVersionErrorMessage(int64_t mpp_version);
int64_t GetMppVersion();
} // namespace TiDB