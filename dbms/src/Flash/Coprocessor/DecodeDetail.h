// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <common/types.h>

namespace DB
{
/// Detail of the packet that decoding in TiRemoteInputStream.RemoteReader.decodeChunks()
struct DecodeDetail
{
    // Responding packets count, usually be 1, be 0 when flush data before eof
    Int64 packets = 1;

    // For fine grained shuffle, each ExchangeReceiver/thread will decode its own blocks.
    // So this is the row number of partial blocks of the original packet.
    // This will be the row number of all blocks of the original packet if it's not fine grained shuffle.
    Int64 rows = 0;

    // Total byte size of the origin packet. When fine grained shuffle is enabled, only records once.
    Int64 packet_bytes = 0;
};
} // namespace DB
