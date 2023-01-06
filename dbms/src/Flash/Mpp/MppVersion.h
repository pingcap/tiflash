#pragma once

#include <string>

namespace DB
{
enum MppVersion : int64_t
{
    MppVersionV0 = 0,
    MppVersionV1,
    //
    MppVersionMAX,
};

enum MPPDataPacketVersion : int64_t
{
    MPPDataPacketV0 = 0,
    MPPDataPacketV1,
    MPPDataPacketMAX,
};

bool CheckMppVersion(int64_t mpp_version);
std::string GenMppVersionErrorMessage(int64_t mpp_version);
int64_t GetMppVersion();

} // namespace DB