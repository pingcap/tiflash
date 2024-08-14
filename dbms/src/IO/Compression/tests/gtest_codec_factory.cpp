// Copyright 2024 PingCAP, Inc.
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

#include <Common/config.h>
#include <IO/Compression/CompressionCodecFactory.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

#include <limits>


namespace DB::tests
{

TEST(TestCodecFactory, TestBasic)
try
{
    {
        auto codec = CompressionCodecFactory::create(
            CompressionSettings(CompressionMethod::LZ4, std::numeric_limits<int>::min()));
        ASSERT_TRUE(codec != nullptr);
    }
    {
        auto codec = CompressionCodecFactory::create(CompressionSettings(CompressionMethod::LZ4, -1));
        ASSERT_TRUE(codec != nullptr);
    }
    {
        auto codec = CompressionCodecFactory::create(CompressionSettings(CompressionMethod::LZ4, 0));
        ASSERT_TRUE(codec != nullptr);
    }
    {
        auto codec = CompressionCodecFactory::create(CompressionSettings(CompressionMethod::LZ4, 1));
        ASSERT_TRUE(codec != nullptr);
    }
    {
        auto codec = CompressionCodecFactory::create(
            CompressionSettings(CompressionMethod::LZ4, std::numeric_limits<int>::max()));
        ASSERT_TRUE(codec != nullptr);
    }
    {
        auto codec = CompressionCodecFactory::create(
            CompressionSettings(CompressionMethod::LZ4HC, std::numeric_limits<int>::min()));
        ASSERT_TRUE(codec != nullptr);
    }
    {
        auto codec = CompressionCodecFactory::create(CompressionSettings(CompressionMethod::LZ4HC, -1));
        ASSERT_TRUE(codec != nullptr);
    }
    {
        auto codec = CompressionCodecFactory::create(CompressionSettings(CompressionMethod::LZ4HC, 0));
        ASSERT_TRUE(codec != nullptr);
    }
    {
        auto codec = CompressionCodecFactory::create(CompressionSettings(CompressionMethod::LZ4HC, 1));
        ASSERT_TRUE(codec != nullptr);
    }
    {
        auto codec = CompressionCodecFactory::create(
            CompressionSettings(CompressionMethod::LZ4HC, std::numeric_limits<int>::max()));
        ASSERT_TRUE(codec != nullptr);
    }
    {
        auto codec = CompressionCodecFactory::create(
            CompressionSettings(CompressionMethod::ZSTD, std::numeric_limits<int>::min()));
        ASSERT_TRUE(codec != nullptr);
    }
    {
        auto codec = CompressionCodecFactory::create(CompressionSettings(CompressionMethod::ZSTD, -1));
        ASSERT_TRUE(codec != nullptr);
    }
    {
        auto codec = CompressionCodecFactory::create(CompressionSettings(CompressionMethod::ZSTD, 0));
        ASSERT_TRUE(codec != nullptr);
    }
    {
        auto codec = CompressionCodecFactory::create(CompressionSettings(CompressionMethod::ZSTD, 1));
        ASSERT_TRUE(codec != nullptr);
    }
    {
        auto codec = CompressionCodecFactory::create(
            CompressionSettings(CompressionMethod::ZSTD, std::numeric_limits<int>::max()));
        ASSERT_TRUE(codec != nullptr);
    }
    {
        auto codec = CompressionCodecFactory::create(CompressionSettings(CompressionMethod::NONE, 1));
        ASSERT_TRUE(codec != nullptr);
    }
#if USE_QPL
    {
        auto codec = CompressionCodecFactory::create(CompressionSettings(CompressionMethod::DEFLATE_QPL, 1));
        ASSERT_TRUE(codec != nullptr);
    }
#endif
}
CATCH

TEST(TestCodecFactory, TestMultipleCodec)
try
{
    {
        std::vector<CompressionSetting> settings{
            CompressionSetting(CompressionMethodByte::FOR),
        };
        ASSERT_ANY_THROW(CompressionCodecFactory::create(CompressionSettings(settings)));
    }
    {
        std::vector<CompressionSetting> settings{
            CompressionSetting(CompressionMethodByte::DeltaFOR),
            CompressionSetting(CompressionMethodByte::RunLength),
        };
        ASSERT_ANY_THROW(CompressionCodecFactory::create(CompressionSettings(settings)));
    }
    {
        std::vector<CompressionSetting> settings{
            CompressionSetting(CompressionMethodByte::DeltaFOR),
            CompressionSetting(CompressionMethodByte::LZ4),
        };
        auto codec = CompressionCodecFactory::create(CompressionSettings(settings));
        ASSERT_TRUE(codec != nullptr);
        ASSERT_TRUE(codec->isCompression());
    }
    {
        std::vector<CompressionSetting> settings{
            CompressionSetting(CompressionMethodByte::ZSTD),
        };
        auto codec = CompressionCodecFactory::create(CompressionSettings(settings));
        ASSERT_TRUE(codec != nullptr);
        ASSERT_TRUE(codec->isCompression());
    }
}
CATCH

} // namespace DB::tests
