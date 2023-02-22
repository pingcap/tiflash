
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

#include <Storages/S3/S3Filename.h>
#include <gtest/gtest.h>

#include <magic_enum.hpp>

namespace DB::S3::tests
{
TEST(S3FilenameTest, Manifest)
{
    UInt64 test_store_id = 1027;
    UInt64 test_seq = 20;
    String fullkey = "s1027/manifest/mf_20";
    auto check = [&](const S3FilenameView & view) {
        ASSERT_EQ(view.type, S3FilenameType::CheckpointManifest) << magic_enum::enum_name(view.type);
        ASSERT_EQ(view.store_id, test_store_id);
        ASSERT_EQ(view.data_subpath, "mf_20");

        ASSERT_EQ(view.toFullKey(), fullkey);

        ASSERT_FALSE(view.isDataFile());
        ASSERT_FALSE(view.isLockFile());

        ASSERT_EQ(view.getUploadSequence(), test_seq);
    };

    auto view = S3FilenameView::fromKey(fullkey);
    check(view);

    {
        auto r = S3Filename::newCheckpointManifest(test_store_id, test_seq);
        ASSERT_EQ(r.toFullKey(), fullkey);
        check(r.toView());
    }
}

TEST(S3FilenameTest, CheckpointDataFile)
{
    UInt64 test_store_id = 2077;
    UInt64 test_seq = 99;
    UInt64 test_file_idx = 1;
    String fullkey = "s2077/data/dat_99_1";
    auto check = [&](const S3FilenameView & view) {
        ASSERT_EQ(view.type, S3FilenameType::DataFile) << magic_enum::enum_name(view.type);
        ASSERT_EQ(view.store_id, test_store_id);
        ASSERT_EQ(view.data_subpath, "dat_99_1");

        ASSERT_EQ(view.toFullKey(), fullkey);

        ASSERT_TRUE(view.isDataFile());
        ASSERT_EQ(view.getLockKey(1234, 50), "lock/s2077/dat_99_1.lock_s1234_50");
        ASSERT_EQ(view.getLockPrefix(), "lock/s2077/dat_99_1.lock_"); // prefix for S3 LIST
        ASSERT_EQ(view.getDelMarkKey(), "s2077/data/dat_99_1.del");
        // ASSERT_EQ(view.getUploadSequence(), test_seq); not used

        ASSERT_FALSE(view.isLockFile());

        // test lockkey for checkpoint data file
        const auto lockkey = view.getLockKey(1234, 50);
        const auto lock_view = S3FilenameView::fromKey(lockkey);
        ASSERT_EQ(lock_view.type, S3FilenameType::LockFile) << magic_enum::enum_name(view.type);
        ASSERT_EQ(lock_view.store_id, test_store_id);
        ASSERT_EQ(String(lock_view.data_subpath), "dat_99_1");

        ASSERT_FALSE(lock_view.isDataFile());
        ASSERT_TRUE(lock_view.isLockFile());
        const auto lock_info = lock_view.getLockInfo();
        ASSERT_EQ(lock_info.store_id, 1234);
        ASSERT_EQ(lock_info.sequence, 50);

        // test delmark
        auto delmark_view = S3FilenameView::fromKey(view.getDelMarkKey());
        ASSERT_TRUE(delmark_view.isDelMark());
    };

    auto view = S3FilenameView::fromKey(fullkey);
    check(view);

    auto r = S3Filename::newCheckpointData(test_store_id, test_seq, test_file_idx);
    ASSERT_EQ(r.toFullKey(), fullkey);
    check(r.toView());
}

TEST(S3FilenameTest, StableFile)
{
    UInt64 test_store_id = 2077;
    String fullkey = "s2077/data/t_44/dmf_57";
    auto check = [&](const S3FilenameView & view) {
        ASSERT_EQ(view.type, S3FilenameType::DataFile) << magic_enum::enum_name(view.type);
        ASSERT_EQ(view.store_id, test_store_id);
        ASSERT_EQ(view.data_subpath, "t_44/dmf_57");

        ASSERT_EQ(view.toFullKey(), fullkey);

        ASSERT_TRUE(view.isDataFile());
        ASSERT_EQ(view.getLockKey(1234, 50), "lock/s2077/t_44/dmf_57.lock_s1234_50");
        ASSERT_EQ(view.getLockPrefix(), "lock/s2077/t_44/dmf_57.lock_"); // prefix for S3 LIST
        ASSERT_EQ(view.getDelMarkKey(), "s2077/data/t_44/dmf_57.del");

        ASSERT_FALSE(view.isLockFile());

        // test lockkey for stable file
        const auto lockkey = view.getLockKey(1234, 50);
        const auto lock_view = S3FilenameView::fromKey(lockkey);
        ASSERT_EQ(lock_view.type, S3FilenameType::LockFile) << magic_enum::enum_name(view.type);
        ASSERT_EQ(lock_view.store_id, test_store_id);
        ASSERT_EQ(String(lock_view.data_subpath), "t_44/dmf_57");

        ASSERT_FALSE(lock_view.isDataFile());
        ASSERT_TRUE(lock_view.isLockFile());
        const auto lock_info = lock_view.getLockInfo();
        ASSERT_EQ(lock_info.store_id, 1234);
        ASSERT_EQ(lock_info.sequence, 50);

        // test delmark
        auto delmark_view = S3FilenameView::fromKey(view.getDelMarkKey());
        ASSERT_TRUE(delmark_view.isDelMark());
    };
    auto view = S3FilenameView::fromKey(fullkey);
    check(view);

    DMFileOID oid{.write_node_id = test_store_id, .table_id = 44, .file_id = 57};
    auto r = S3Filename::fromDMFileOID(oid);
    ASSERT_EQ(r.toFullKey(), fullkey);
    check(r.toView());
}

TEST(S3FilenameTest, StorePrefix)
{
    {
        auto r = S3FilenameView::fromStoreKeyPrefix("s5/");
        ASSERT_EQ(r.type, S3FilenameType::StorePrefix);
        ASSERT_EQ(r.store_id, 5);
        ASSERT_EQ(r.toFullKey(), "s5/");
    }
    {
        auto r = S3Filename::fromStoreId(5);
        ASSERT_EQ(r.toFullKey(), "s5/");
        ASSERT_EQ(r.toManifestPrefix(), "s5/manifest/");
        ASSERT_EQ(r.toDataPrefix(), "s5/data/");
    }

    {
        auto r = S3FilenameView::fromStoreKeyPrefix("s1024/");
        ASSERT_EQ(r.type, S3FilenameType::StorePrefix);
        ASSERT_EQ(r.store_id, 1024);
        ASSERT_EQ(r.toFullKey(), "s1024/");
    }
    {
        auto r = S3Filename::fromStoreId(1024);
        ASSERT_EQ(r.toFullKey(), "s1024/");
        ASSERT_EQ(r.toManifestPrefix(), "s1024/manifest/");
        ASSERT_EQ(r.toDataPrefix(), "s1024/data/");
    }
}

} // namespace DB::S3::tests
