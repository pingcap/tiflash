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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <gtest/gtest.h>
#pragma GCC diagnostic pop
#include <Common/Checksum.h>
#include <Storages/DeltaMerge/DMChecksumConfig.h>
namespace DB::DM
{
template <ChecksumAlgo algo>
void runSerializationTest()
{
    DMChecksumConfig original{{{"abc", "abc"}, {"123", "123"}}, TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE, algo};

    std::stringstream ss;
    ss << original;

    DMChecksumConfig deserialized(ss);

    ASSERT_EQ(original.getChecksumAlgorithm(), deserialized.getChecksumAlgorithm());
    ASSERT_EQ(original.getChecksumFrameLength(), deserialized.getChecksumFrameLength());
    ASSERT_EQ(original.getDebugInfo(), deserialized.getDebugInfo());
    ASSERT_EQ(original.getEmbeddedChecksum(), deserialized.getEmbeddedChecksum());
};

#define TEST_SERIALIZATION(ALGO) \
    TEST(DMConfiguration, ALGO##Serialization) { runSerializationTest<DB::ChecksumAlgo::ALGO>(); }

TEST_SERIALIZATION(None)
TEST_SERIALIZATION(CRC32)
TEST_SERIALIZATION(CRC64)
TEST_SERIALIZATION(City128)
TEST_SERIALIZATION(XXH3)

} // namespace DB::DM
