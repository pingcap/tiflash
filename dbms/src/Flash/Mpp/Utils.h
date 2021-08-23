#pragma once

#include <common/types.h>
#include <kvproto/mpp.pb.h>

namespace DB
{

mpp::MPPDataPacket getPacketWithError(String reason);

} // namespace DB

