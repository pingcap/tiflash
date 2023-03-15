// Copyright 2022 PingCAP, Ltd.
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

#include <Storages/S3/S3Common.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <gtest/gtest.h>

namespace DB::S3::tests
{

class S3ClientTest : public ::testing::Test
{
public:
    void SetUp() override
    {
        client = ClientFactory::instance().sharedTiFlashClient();
        ::DB::tests::TiFlashTestEnv::deleteBucket(*client);
        ::DB::tests::TiFlashTestEnv::createBucketIfNotExist(*client);
    }

    std::shared_ptr<TiFlashS3Client> client;
};

TEST_F(S3ClientTest, UploadRead)
try
{
    deleteObject(*client, "s999/manifest/mf_1");
    ASSERT_FALSE(objectExists(*client, "s999/manifest/mf_1"));
    uploadEmptyFile(*client, "s999/manifest/mf_1");
    ASSERT_TRUE(objectExists(*client, "s999/manifest/mf_1"));

    uploadEmptyFile(*client, "s999/manifest/mf_2");
    uploadEmptyFile(*client, "s999/manifest/mf_789");

    uploadEmptyFile(*client, "s999/data/dat_789_0");
    uploadEmptyFile(*client, "s999/data/dat_790_0");

    Strings prefixes;
    listPrefixWithDelimiter(*client, "s999/", "/", [&](const Aws::S3::Model::CommonPrefix & p) {
        prefixes.emplace_back(p.GetPrefix());
        return PageResult{.num_keys = 1, .more = true};
    });
    EXPECT_FALSE(true) << fmt::format("{}", prefixes);

    prefixes.clear();
    listPrefix(*client, "s999/", "/", [&](const Aws::S3::Model::ListObjectsV2Result & result, const String &) {
        for (const auto & prefix : result.GetCommonPrefixes())
        {
            prefixes.emplace_back(prefix.GetPrefix());
        }
        return PageResult{.num_keys = result.GetCommonPrefixes().size(), .more = true};
    });
    EXPECT_FALSE(true) << fmt::format("{}", prefixes);
}
CATCH

} // namespace DB::S3::tests
