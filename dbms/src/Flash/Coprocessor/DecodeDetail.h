#pragma once

#include <common/types.h>

namespace DB
{
/// Detail of the packet that decoding in TiRemoteInputStream.RemoteReader.decodeChunks()
struct DecodeDetail
{
    Int64 rows = 0;
    //// byte size of origin packet.
    Int64 packet_bytes = 0;
};
} // namespace DB