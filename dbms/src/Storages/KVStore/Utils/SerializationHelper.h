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

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/enginepb.pb.h>
#pragma GCC diagnostic pop

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

template <typename T, bool ok = std::is_arithmetic_v<T>>
size_t writeBinary2(const T & x, WriteBuffer & buf)
{
    if constexpr (ok)
    {
        buf.write(reinterpret_cast<const char *>(&x), sizeof(x));
        return sizeof(x);
    }
    throw Exception("Unimplemented (writeBinary2)", ErrorCodes::LOGICAL_ERROR);
}

template <typename T, bool ok = std::is_arithmetic_v<T>>
inline T readBinary2(ReadBuffer & buf)
{
    if constexpr (ok)
    {
        T t;
        readPODBinary(t, buf);
        return t;
    }
    throw Exception("Unimplemented (readBinary2)", ErrorCodes::LOGICAL_ERROR);
}

inline size_t writeBinary2(const std::string & s, WriteBuffer & buf)
{
    writeIntBinary(static_cast<UInt32>(s.size()), buf);
    buf.write(s.data(), s.size());
    return 4 + s.size();
}

template <>
inline std::string readBinary2<std::string>(ReadBuffer & buf)
{
    UInt32 size = 0;
    readIntBinary(size, buf);

    if (size > DEFAULT_MAX_STRING_SIZE)
        throw Exception("Too large string size.", ErrorCodes::LOGICAL_ERROR);
    std::string s;
    s.resize(size);
    buf.readStrict(&s[0], size);
    return s;
}

inline std::string readStringWithLength(ReadBuffer & buf, size_t length)
{
    std::string s;
    s.resize(length);
    buf.readStrict(&s[0], length);
    return s;
}

size_t writeBinary2(const metapb::Peer & peer, WriteBuffer & buf);
size_t writeBinary2(const metapb::Region & region, WriteBuffer & buf);
size_t writeBinary2(const raft_serverpb::RaftApplyState & state, WriteBuffer & buf);
size_t writeBinary2(const raft_serverpb::RegionLocalState & state, WriteBuffer & buf);
size_t writeBinary2(const raft_serverpb::MergeState & state, WriteBuffer & buf);

metapb::Peer readPeer(ReadBuffer & buf);
metapb::Region readRegion(ReadBuffer & buf);
raft_serverpb::RaftApplyState readApplyState(ReadBuffer & buf);
raft_serverpb::RegionLocalState readRegionLocalState(ReadBuffer & buf);
raft_serverpb::MergeState readMergeState(ReadBuffer & buf);

bool operator==(const metapb::Peer & peer1, const metapb::Peer & peer2);
bool operator==(const metapb::Region & region1, const metapb::Region & region2);
bool operator==(const raft_serverpb::RaftApplyState & state1, const raft_serverpb::RaftApplyState & state2);
bool operator==(const raft_serverpb::RegionLocalState & state1, const raft_serverpb::RegionLocalState & state2);
bool operator==(const raft_serverpb::MergeState & state1, const raft_serverpb::MergeState & state2);

} // namespace DB
