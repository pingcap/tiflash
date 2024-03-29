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

#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/Compression/CompressedReadBuffer.h>
#include <IO/Compression/CompressedWriteBuffer.h>
#include <Storages/Page/V3/CheckpointFile/Proto/manifest_file.pb.h>
#include <Storages/Page/V3/CheckpointFile/ProtoHelper.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::PS::V3::tests
{

TEST(ProtoHelperTest, WriteAndRead)
try
{
    WriteBufferFromOwnString buf;
    auto writer = std::make_unique<CompressedWriteBuffer<true>>(buf, CompressionSettings());

    CheckpointProto::EditRecord r;
    r.set_page_id("foo");
    r.set_version_epoch(4);
    details::writeMessageWithLength(*writer, r);

    r.Clear();
    r.set_page_id("bar box");
    details::writeMessageWithLength(*writer, r);
    writer->next();

    ReadBufferFromString read_buf(buf.str());
    auto reader = std::make_unique<CompressedReadBuffer<true>>(read_buf);

    CheckpointProto::EditRecord r2;
    details::readMessageWithLength(*reader, r2);
    ASSERT_EQ("foo", r2.page_id());
    ASSERT_EQ(4, r2.version_epoch());

    r2.Clear();
    details::readMessageWithLength(*reader, r2);
    ASSERT_EQ("bar box", r2.page_id());
    ASSERT_EQ(0, r2.version_epoch());
}
CATCH

} // namespace DB::PS::V3::tests
