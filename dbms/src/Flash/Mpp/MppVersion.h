#pragma once

#include <string>

namespace TiDB
{
enum MppVersion : int64_t
{
    MppVersionV0 = 0,
    MppVersionV1,
    MppVersionMAX,
};

bool CheckMppVersion(int64_t mpp_version);
std::string GenMppVersionErrorMessage(int64_t mpp_version);
int64_t GetMppVersion();
std::string GetMppVersionReleaseInfo(int64_t mpp_version);
} // namespace TiDB