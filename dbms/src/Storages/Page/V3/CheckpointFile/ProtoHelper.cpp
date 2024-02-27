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

#include <IO/IOSWrapper.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/V3/CheckpointFile/ProtoHelper.h>

namespace DB::PS::V3::details
{

constexpr UInt64 MAX_SIZE = 1UL * 1024 * 1024 * 1024;

void readMessageWithLength(ReadBuffer & reader, google::protobuf::MessageLite & msg_out)
{
    UInt64 prefix_size = 0;
    readIntBinary(prefix_size, reader);

    // Avoid corrupted data causing OOMs.
    RUNTIME_CHECK_MSG(prefix_size < MAX_SIZE, "Expect total message to be < 1GiB, size={}", prefix_size);

    String buf;
    buf.resize(prefix_size);
    reader.readBig(buf.data(), prefix_size);

    bool ok = msg_out.ParseFromArray(buf.data(), prefix_size);
    RUNTIME_CHECK(ok);
}

void writeMessageWithLength(WriteBuffer & writer, const google::protobuf::MessageLite & msg)
{
    UInt64 prefix_size = msg.ByteSizeLong();
    writeIntBinary(prefix_size, writer);

    auto sz = writer.count();
    OutputStreamWrapper ostream{writer};
    bool ok = msg.SerializeToOstream(&ostream);

    auto sz2 = writer.count();
    RUNTIME_CHECK(ok);
    RUNTIME_CHECK(UInt64(sz2 - sz) == prefix_size, sz2 - sz, prefix_size);
}

} // namespace DB::PS::V3::details