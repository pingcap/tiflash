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

#include <Storages/Page/V3/CheckpointFile/CPFilesWriter.h>
#include <Storages/Page/V3/CheckpointFile/CPManifestFileReader.h>
#include <Storages/Page/V3/CheckpointFile/CPWriteDataSource.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::PS::V3::tests
{

class CheckpointFileTest : public ::testing::Test
{
public:
    void SetUp() override
    {
        dir = ::DB::tests::TiFlashTestEnv::getTemporaryPath("/CheckpointFileTest/");
        Poco::File file(dir);
        if (file.exists())
            file.remove(true);
        file.createDirectories();
    }

    std::string readData(const V3::CheckpointLocation & location)
    {
        RUNTIME_CHECK(location.offset_in_file > 0);
        RUNTIME_CHECK(location.data_file_id != nullptr && !location.data_file_id->empty());

        std::string ret;
        ret.resize(location.size_in_file);

        auto buf = ReadBufferFromFile(dir + *location.data_file_id);
        buf.seek(location.offset_in_file);
        auto n = buf.readBig(ret.data(), location.size_in_file);
        RUNTIME_CHECK(n == location.size_in_file);

        return ret;
    }

protected:
    std::string dir;
};

TEST_F(CheckpointFileTest, WritePrefixOnly)
try
{
    auto writer = CPFilesWriter::create({
        .data_file_path = dir + "data_1",
        .data_file_id = "data_1",
        .manifest_file_path = dir + "manifest_foo",
        .manifest_file_id = "manifest_foo",
        .data_source = CPWriteDataSourceFixture::create({}),
    });

    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });

    writer.reset();

    ASSERT_TRUE(Poco::File(dir + "data_1").exists());
    ASSERT_TRUE(Poco::File(dir + "manifest_foo").exists());

    auto manifest_reader = CPManifestFileReader::create({
        .file_path = dir + "manifest_foo",
    });
    auto prefix = manifest_reader->readPrefix();
    ASSERT_EQ(5, prefix.local_sequence());
    ASSERT_EQ(3, prefix.last_local_sequence());
}
CATCH

TEST_F(CheckpointFileTest, WriteEditsWithoutPrefix)
try
{
    auto writer = CPFilesWriter::create({
        .data_file_path = dir + "data_1",
        .data_file_id = "data_1",
        .manifest_file_path = dir + "manifest_foo",
        .manifest_file_id = "manifest_foo",
        .data_source = CPWriteDataSourceFixture::create({}),
    });

    auto edits = universal::PageEntriesEdit{};
    edits.appendRecord({.type = EditRecordType::DEL});

    ASSERT_THROW({
        writer->writeEditsAndApplyRemoteInfo(edits);
    },
                 DB::Exception);
}
CATCH

TEST_F(CheckpointFileTest, WriteEdits)
try
{
    auto writer = CPFilesWriter::create({
        .data_file_path = dir + "data_1",
        .data_file_id = "data_1",
        .manifest_file_path = dir + "manifest_foo",
        .manifest_file_id = "manifest_foo",
        .data_source = CPWriteDataSourceFixture::create({}),
    });

    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });
    {
        auto edits = universal::PageEntriesEdit{};
        edits.appendRecord({.type = EditRecordType::VAR_DELETE});
        writer->writeEditsAndApplyRemoteInfo(edits);
    }
    writer->writeSuffix();
    writer.reset();

    auto manifest_reader = CPManifestFileReader::create({
        .file_path = dir + "manifest_foo",
    });
    auto prefix = manifest_reader->readPrefix();
    CheckpointProto::StringsInternMap im;
    {
        auto edits_r = manifest_reader->readEdits(im);
        ASSERT_TRUE(edits_r.has_value());
        ASSERT_EQ(1, edits_r->size());
        ASSERT_EQ(EditRecordType::VAR_DELETE, edits_r->getRecords()[0].type);
    }
    {
        auto edits_r = manifest_reader->readEdits(im);
        ASSERT_FALSE(edits_r.has_value());
    }
    {
        auto edits_r = manifest_reader->readEdits(im);
        ASSERT_FALSE(edits_r.has_value());
    }
}
CATCH

TEST_F(CheckpointFileTest, WriteMultipleEdits)
try
{
    auto writer = CPFilesWriter::create({
        .data_file_path = dir + "data_1",
        .data_file_id = "data_1",
        .manifest_file_path = dir + "manifest_foo",
        .manifest_file_id = "manifest_foo",
        .data_source = CPWriteDataSourceFixture::create({{5, "Said she just dreamed a dream"},
                                                         {10, "nahida opened her eyes"}}),
    });

    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });
    {
        auto edits = universal::PageEntriesEdit{};
        edits.appendRecord({.type = EditRecordType::VAR_DELETE});
        writer->writeEditsAndApplyRemoteInfo(edits);
    }
    {
        auto edits = universal::PageEntriesEdit{};
        edits.appendRecord({.type = EditRecordType::VAR_ENTRY, .page_id = "abc", .entry = {.offset = 5}});
        edits.appendRecord({.type = EditRecordType::VAR_REF, .page_id = "foo", .ori_page_id = "abc"});
        edits.appendRecord({.type = EditRecordType::VAR_ENTRY, .page_id = "aaabbb", .entry = {.offset = 10}});
        edits.appendRecord({.type = EditRecordType::VAR_DELETE});
        writer->writeEditsAndApplyRemoteInfo(edits);
    }
    writer->writeSuffix();
    writer.reset();

    auto manifest_reader = CPManifestFileReader::create({
        .file_path = dir + "manifest_foo",
    });
    manifest_reader->readPrefix();
    CheckpointProto::StringsInternMap im;
    {
        auto edits_r = manifest_reader->readEdits(im);
        ASSERT_TRUE(edits_r.has_value());
        ASSERT_EQ(1, edits_r->size());
        ASSERT_EQ(EditRecordType::VAR_DELETE, edits_r->getRecords()[0].type);
    }
    {
        auto edits_r = manifest_reader->readEdits(im);
        auto r = edits_r->getRecords();
        ASSERT_EQ(4, r.size());

        ASSERT_EQ(EditRecordType::VAR_ENTRY, r[0].type);
        ASSERT_EQ("abc", r[0].page_id);
        ASSERT_EQ(0, r[0].entry.offset); // The deserialized offset is not the same as the original one!
        ASSERT_EQ(0, r[0].entry.size);
        ASSERT_TRUE(r[0].entry.checkpoint_info->is_local_data_reclaimed);
        ASSERT_EQ("data_1", *r[0].entry.checkpoint_info->data_location.data_file_id);
        ASSERT_EQ("Said she just dreamed a dream", readData(r[0].entry.checkpoint_info->data_location));

        ASSERT_EQ(EditRecordType::VAR_REF, r[1].type);
        ASSERT_EQ("foo", r[1].page_id);
        ASSERT_EQ("abc", r[1].ori_page_id);

        ASSERT_EQ(EditRecordType::VAR_ENTRY, r[2].type);
        ASSERT_EQ("aaabbb", r[2].page_id);
        ASSERT_EQ(0, r[2].entry.offset);
        ASSERT_EQ(0, r[2].entry.size);
        ASSERT_TRUE(r[2].entry.checkpoint_info->is_local_data_reclaimed);
        ASSERT_EQ("data_1", *r[2].entry.checkpoint_info->data_location.data_file_id);
        ASSERT_EQ("nahida opened her eyes", readData(r[2].entry.checkpoint_info->data_location));

        ASSERT_EQ(EditRecordType::VAR_DELETE, r[3].type);

        // Check data_file_id is shared.
        ASSERT_EQ(
            r[0].entry.checkpoint_info->data_location.data_file_id->data(),
            r[2].entry.checkpoint_info->data_location.data_file_id->data());
    }
    {
        auto edits_r = manifest_reader->readEdits(im);
        ASSERT_FALSE(edits_r.has_value());
    }
}
CATCH

TEST_F(CheckpointFileTest, WriteEditsWithCheckpointInfo)
try
{
    auto writer = CPFilesWriter::create({
        .data_file_path = dir + "data_1",
        .data_file_id = "data_1",
        .manifest_file_path = dir + "manifest_foo",
        .manifest_file_id = "manifest_foo",
        .data_source = CPWriteDataSourceFixture::create({{10, "nahida opened her eyes"}}),
    });

    writer->writePrefix({
        .writer = {},
        .sequence = 5,
        .last_sequence = 3,
    });
    {
        auto edits = universal::PageEntriesEdit{};
        edits.appendRecord({
            .type = EditRecordType::VAR_ENTRY,
            .page_id = "abc",
            .entry = {
                .offset = 5,
                .checkpoint_info = CheckpointInfo{
                    .data_location = {
                        .data_file_id = std::make_shared<String>("my_file_id"),
                    },
                    .is_local_data_reclaimed = false,
                },
            },
        });
        edits.appendRecord({.type = EditRecordType::VAR_REF, .page_id = "foo", .ori_page_id = "abc"});
        edits.appendRecord({.type = EditRecordType::VAR_ENTRY, .page_id = "aaabbb", .entry = {.offset = 10}});
        edits.appendRecord({.type = EditRecordType::VAR_DELETE});
        writer->writeEditsAndApplyRemoteInfo(edits);
    }
    writer->writeSuffix();
    writer.reset();

    auto manifest_reader = CPManifestFileReader::create({
        .file_path = dir + "manifest_foo",
    });
    manifest_reader->readPrefix();
    CheckpointProto::StringsInternMap im;
    {
        auto edits_r = manifest_reader->readEdits(im);
        auto r = edits_r->getRecords();
        ASSERT_EQ(4, r.size());

        ASSERT_EQ(EditRecordType::VAR_ENTRY, r[0].type);
        ASSERT_EQ("abc", r[0].page_id);
        ASSERT_EQ(0, r[0].entry.offset); // The deserialized offset is not the same as the original one!
        ASSERT_EQ(0, r[0].entry.size);
        ASSERT_TRUE(r[0].entry.checkpoint_info->is_local_data_reclaimed); // After deserialization, this field is always true!
        ASSERT_EQ("my_file_id", *r[0].entry.checkpoint_info->data_location.data_file_id);

        ASSERT_EQ(EditRecordType::VAR_REF, r[1].type);
        ASSERT_EQ("foo", r[1].page_id);
        ASSERT_EQ("abc", r[1].ori_page_id);

        ASSERT_EQ(EditRecordType::VAR_ENTRY, r[2].type);
        ASSERT_EQ("aaabbb", r[2].page_id);
        ASSERT_EQ(0, r[2].entry.offset);
        ASSERT_EQ(0, r[2].entry.size);
        ASSERT_TRUE(r[2].entry.checkpoint_info->is_local_data_reclaimed);
        ASSERT_EQ("data_1", *r[2].entry.checkpoint_info->data_location.data_file_id);
        ASSERT_EQ("nahida opened her eyes", readData(r[2].entry.checkpoint_info->data_location));

        ASSERT_EQ(EditRecordType::VAR_DELETE, r[3].type);
    }
    {
        auto edits_r = manifest_reader->readEdits(im);
        ASSERT_FALSE(edits_r.has_value());
    }
}
CATCH

} // namespace DB::PS::V3::tests
