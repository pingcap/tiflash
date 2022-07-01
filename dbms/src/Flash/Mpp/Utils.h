#pragma once

#include <common/types.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/mpp.pb.h>
#pragma GCC diagnostic pop

namespace DB
{
mpp::MPPDataPacket getPacketWithError(String reason);

} // namespace DB
