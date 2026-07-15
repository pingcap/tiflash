// Copyright 2026 PingCAP, Inc.
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

#include <Common/MyTime.h>
#include <Common/PODArray.h>
#include <Common/StringUtils/StringUtils.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/Encryption/MockKeyManager.h>
#include <Interpreters/Context.h>
#include <Interpreters/Settings.h>
#include <Storages/DeltaMerge/File/ColumnStat.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileMetaV2.h>
#include <Storages/DeltaMerge/File/DMFileUtil.h>
#include <Storages/DeltaMerge/File/DMFileWriter.h>
#include <Storages/DeltaMerge/File/MergedFile.h>
#include <Storages/DeltaMerge/Index/MinMaxIndex.h>
#include <Storages/DeltaMerge/Index/TrimMinMaxIndex.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB::DM::tests
{
namespace
{
dtpb::TrimMinMaxIndexProps makeProps(UInt32 version, UInt64 lower, UInt64 upper, UInt64 pack_count)
{
    dtpb::TrimMinMaxIndexProps props;
    props.set_format_version(version);
    props.set_lower_bound(TrimMinMax::encodeBound(lower));
    props.set_upper_bound(TrimMinMax::encodeBound(upper));
    props.set_pack_count(pack_count);
    return props;
}
} // namespace

TEST(TrimMinMaxIndexPhaseA, PackMarkAccessors)
{
    PaddedPODArray<UInt8> pack_marks;
    PaddedPODArray<UInt8> has_value_marks;
    auto type = std::make_shared<DataTypeMyDateTime>(0);
    auto minmaxes = type->createColumn();

    const UInt8 marks[] = {0x00, 0x01, 0x02, 0x04, 0x06, 0x07};
    for (UInt8 mark : marks)
    {
        pack_marks.push_back(mark);
        has_value_marks.push_back(1);
        minmaxes->insert(Field(static_cast<UInt64>(1)));
        minmaxes->insert(Field(static_cast<UInt64>(2)));
    }

    auto index = std::make_shared<MinMaxIndex>(std::move(pack_marks), std::move(has_value_marks), std::move(minmaxes));

    EXPECT_FALSE(index->hasNull(0));
    EXPECT_FALSE(index->hasTrimmedLow(0));
    EXPECT_FALSE(index->hasTrimmedHigh(0));

    EXPECT_TRUE(index->hasNull(1));
    EXPECT_FALSE(index->hasTrimmedLow(1));
    EXPECT_FALSE(index->hasTrimmedHigh(1));

    EXPECT_FALSE(index->hasNull(2));
    EXPECT_TRUE(index->hasTrimmedLow(2));
    EXPECT_FALSE(index->hasTrimmedHigh(2));

    EXPECT_FALSE(index->hasNull(3));
    EXPECT_FALSE(index->hasTrimmedLow(3));
    EXPECT_TRUE(index->hasTrimmedHigh(3));

    EXPECT_FALSE(index->hasNull(4));
    EXPECT_TRUE(index->hasTrimmedLow(4));
    EXPECT_TRUE(index->hasTrimmedHigh(4));

    EXPECT_TRUE(index->hasNull(5));
    EXPECT_TRUE(index->hasTrimmedLow(5));
    EXPECT_TRUE(index->hasTrimmedHigh(5));

    // Ordinary NULL checks must ignore trim bits (bit1/bit2 must not look like NULL).
    EXPECT_FALSE(hasNullMark(0x02));
    EXPECT_FALSE(hasNullMark(0x04));
    EXPECT_FALSE(hasNullMark(0x06));
    EXPECT_TRUE(hasNullMark(0x07));
}

TEST(TrimMinMaxIndexPhaseA, ValidateTrimPackMarks)
{
    PaddedPODArray<UInt8> ok;
    ok.push_back(0x00);
    ok.push_back(0x01);
    ok.push_back(0x02);
    ok.push_back(0x04);
    ok.push_back(0x07);
    EXPECT_TRUE(TrimMinMax::validateTrimPackMarks(ok, 5));

    PaddedPODArray<UInt8> bad_reserved;
    bad_reserved.push_back(0x08);
    EXPECT_FALSE(TrimMinMax::validateTrimPackMarks(bad_reserved, 1));

    PaddedPODArray<UInt8> bad_count;
    bad_count.push_back(0x01);
    EXPECT_FALSE(TrimMinMax::validateTrimPackMarks(bad_count, 2));
}

TEST(TrimMinMaxIndexPhaseA, ColumnStatProtoRoundTripAndUnknownField)
{
    auto type = std::make_shared<DataTypeMyDateTime>(0);
    ColumnStat stat{
        .col_id = 42,
        .type = type,
        .avg_size = 8,
        .serialized_bytes = 100,
        .data_bytes = 80,
        .mark_bytes = 8,
        .index_bytes = 20,
        .vector_index = {},
        .trim_minmax_index = TrimMinMax::makeDefaultProps(*type, /*pack_count*/ 3),
    };

    auto proto = stat.toProto();
    ASSERT_TRUE(proto.has_trim_minmax_index());
    EXPECT_EQ(proto.trim_minmax_index().format_version(), TrimMinMax::FormatVersionV1);
    EXPECT_EQ(proto.trim_minmax_index().pack_count(), 3u);
    // Must not land in vector_indexes.
    EXPECT_EQ(proto.vector_indexes_size(), 0);

    ColumnStat restored;
    restored.mergeFromProto(proto);
    ASSERT_TRUE(restored.trim_minmax_index.has_value());
    EXPECT_EQ(restored.trim_minmax_index->pack_count(), 3u);
    EXPECT_EQ(restored.col_id, 42);

    // Simulate an old Reader that does not know field 105:
    // serialize with field 105, then parse and drop trim metadata to mimic an
    // old-node ColumnStat rewrite.
    String bytes;
    ASSERT_TRUE(proto.SerializeToString(&bytes));

    dtpb::ColumnStat old_reader_view;
    ASSERT_TRUE(old_reader_view.ParseFromString(bytes));
    // Current generated code knows field 105; verify wire bytes still round-trip when
    // field 105 is dropped (metadata rewrite by an old node).
    old_reader_view.clear_trim_minmax_index();
    ColumnStat after_old_rewrite;
    after_old_rewrite.mergeFromProto(old_reader_view);
    EXPECT_FALSE(after_old_rewrite.trim_minmax_index.has_value());
}

TEST(TrimMinMaxIndexPhaseA, TrySelectTrimMetaFallbackReasons)
{
    auto type = std::make_shared<DataTypeMyDateTime>(0);
    const auto lower = TrimMinMax::defaultLowerBoundPacked(*type);
    const auto upper = TrimMinMax::defaultUpperBoundPacked(*type);
    const String fname = colTrimIndexFileName("42");

    std::unordered_map<String, MergedSubFileInfo> merged;
    merged.emplace(fname, MergedSubFileInfo(fname, /*number*/ 0, /*offset*/ 10, /*size*/ 100));

    TrimMinMaxIndexMeta meta;
    EXPECT_EQ(
        TrimMinMax::trySelectTrimMeta(
            /*read_enabled*/ false,
            makeProps(1, lower, upper, 2),
            *type,
            /*expected_pack_count*/ 2,
            merged,
            fname,
            &meta),
        TrimMinMaxFallbackReason::Disabled);

    EXPECT_EQ(
        TrimMinMax::trySelectTrimMeta(
            /*read_enabled*/ true,
            std::nullopt,
            *type,
            2,
            merged,
            fname,
            &meta),
        TrimMinMaxFallbackReason::NoMeta);

    EXPECT_EQ(
        TrimMinMax::trySelectTrimMeta(
            /*read_enabled*/ true,
            makeProps(/*version*/ 99, lower, upper, 2),
            *type,
            2,
            merged,
            fname,
            &meta),
        TrimMinMaxFallbackReason::UnsupportedVersion);

    EXPECT_EQ(
        TrimMinMax::trySelectTrimMeta(
            /*read_enabled*/ true,
            makeProps(1, upper, lower, 2), // lower >= upper
            *type,
            2,
            merged,
            fname,
            &meta),
        TrimMinMaxFallbackReason::MetadataMismatch);

    EXPECT_EQ(
        TrimMinMax::trySelectTrimMeta(
            /*read_enabled*/ true,
            makeProps(1, lower, upper, /*pack_count*/ 3),
            *type,
            /*expected*/ 2,
            merged,
            fname,
            &meta),
        TrimMinMaxFallbackReason::MetadataMismatch);

    auto props_ok = makeProps(1, lower, upper, 2);
    EXPECT_EQ(
        TrimMinMax::trySelectTrimMeta(
            /*read_enabled*/ true,
            props_ok,
            *type,
            2,
            /*empty map*/ {},
            fname,
            &meta),
        TrimMinMaxFallbackReason::IndexMissing);

    EXPECT_EQ(
        TrimMinMax::trySelectTrimMeta(/*read_enabled*/ true, props_ok, *type, 2, merged, fname, &meta),
        TrimMinMaxFallbackReason::None);
    EXPECT_EQ(meta.pack_count, 2u);
    EXPECT_EQ(meta.file_size, 100u);
    EXPECT_EQ(meta.lower_bound, lower);
    EXPECT_EQ(meta.upper_bound, upper);

    // Orphan subfile: meta missing but MergedSubFileInfo remains.
    EXPECT_TRUE(TrimMinMax::hasOrphanTrimSubFile(std::nullopt, merged, fname));
    EXPECT_FALSE(TrimMinMax::hasOrphanTrimSubFile(props_ok, merged, fname));
}

TEST(TrimMinMaxIndexPhaseA, FileNamingAndCacheKeyDistinct)
{
    EXPECT_EQ(colIndexFileName("42"), "42.idx");
    EXPECT_EQ(colTrimIndexFileName("42"), "42.trim.idx");
    EXPECT_NE(colIndexFileName("42"), colTrimIndexFileName("42"));
    EXPECT_TRUE(endsWith(colTrimIndexFileName("42"), details::TRIM_INDEX_FILE_SUFFIX));
    EXPECT_TRUE(endsWith(colTrimIndexFileName("42"), details::INDEX_FILE_SUFFIX));
}

TEST(TrimMinMaxIndexPhaseA, SettingsDefaultOff)
{
    Settings settings;
    EXPECT_FALSE(settings.dt_enable_trim_minmax_write);
    EXPECT_FALSE(settings.dt_enable_trim_minmax_read);
}

TEST(TrimMinMaxIndexPhaseB, AddOrdinaryAndTrimPack)
{
    auto type = std::make_shared<DataTypeMyDateTime>(0);
    MinMaxIndex ordinary(*type);
    MinMaxIndex trim(*type);

    const UInt64 lower = TrimMinMax::defaultLowerBoundPacked(*type);
    const UInt64 upper = TrimMinMax::defaultUpperBoundPacked(*type);
    const UInt64 in_range = MyDateTime(2020, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 low_out = MyDateTime(1800, 1, 1, 0, 0, 0, 0).toPackedUInt();
    const UInt64 high_out = MyDateTime(2100, 1, 1, 0, 0, 0, 0).toPackedUInt();

    auto make_col = [&](const std::vector<UInt64> & vals) {
        auto col = type->createColumn();
        for (auto v : vals)
            col->insert(Field(v));
        return col;
    };

    // Pack 0: all in range
    {
        auto col = make_col({in_range, in_range + 1});
        TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *col, nullptr, lower, upper);
        EXPECT_FALSE(ordinary.hasNull(0));
        EXPECT_TRUE(ordinary.getCell(0).has_value);
        EXPECT_FALSE(trim.hasTrimmedLow(0));
        EXPECT_FALSE(trim.hasTrimmedHigh(0));
        EXPECT_TRUE(trim.getCell(0).has_value);
    }

    // Pack 1: high outlier only
    {
        auto col = make_col({high_out});
        TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *col, nullptr, lower, upper);
        EXPECT_TRUE(ordinary.getCell(1).has_value);
        EXPECT_FALSE(trim.getCell(1).has_value);
        EXPECT_FALSE(trim.hasTrimmedLow(1));
        EXPECT_TRUE(trim.hasTrimmedHigh(1));
    }

    // Pack 2: low + high + in-range
    {
        auto col = make_col({low_out, in_range, high_out});
        TrimMinMax::addOrdinaryAndTrimPack(ordinary, trim, *col, nullptr, lower, upper);
        EXPECT_TRUE(trim.getCell(2).has_value);
        EXPECT_TRUE(trim.hasTrimmedLow(2));
        EXPECT_TRUE(trim.hasTrimmedHigh(2));
        EXPECT_EQ(trim.getCell(2).min.safeGet<UInt64>(), in_range);
        EXPECT_EQ(trim.getCell(2).max.safeGet<UInt64>(), in_range);
    }

    // Pack 3: nullable NULL + high outlier
    {
        auto nullable_type = makeNullable(type);
        MinMaxIndex ordinary_n(*nullable_type);
        MinMaxIndex trim_n(*nullable_type);
        auto col = nullable_type->createColumn();
        col->insertDefault(); // NULL
        col->insert(Field(high_out));
        TrimMinMax::addOrdinaryAndTrimPack(ordinary_n, trim_n, *col, nullptr, lower, upper);
        EXPECT_TRUE(ordinary_n.hasNull(0));
        EXPECT_TRUE(trim_n.hasNull(0));
        EXPECT_TRUE(trim_n.hasTrimmedHigh(0));
        EXPECT_FALSE(trim_n.getCell(0).has_value);
        EXPECT_EQ(trim_n.packMark(0), PackMarkBits::Null | PackMarkBits::TrimmedHigh);
    }

    EXPECT_TRUE(trim.hasAnyTrimmedValue());
    EXPECT_FALSE(ordinary.hasAnyTrimmedValue());

    // Serialize / deserialize trim payload
    WriteBufferFromOwnString buf;
    trim.write(*type, buf);
    ReadBufferFromString rbuf(buf.str());
    auto restored = MinMaxIndex::read(*type, rbuf, buf.str().size());
    ASSERT_TRUE(TrimMinMax::validateTrimPackMarks(restored->packMarks(), 3));
    EXPECT_TRUE(restored->hasTrimmedHigh(1));
    EXPECT_TRUE(restored->hasTrimmedLow(2));
    EXPECT_TRUE(restored->hasTrimmedHigh(2));
}

TEST(TrimMinMaxIndexPhaseB, AppendPackRejectsInvalidMask)
{
    auto type = std::make_shared<DataTypeMyDateTime>(0);
    MinMaxIndex index(*type);
    EXPECT_THROW(
        index.appendPack(/*pack_mark*/ 0x08, /*has_value*/ false, PackMarkBits::TrimAllowedMask),
        DB::Exception);
    EXPECT_THROW(
        index.appendPack(/*pack_mark*/ PackMarkBits::TrimmedLow, /*has_value*/ false, PackMarkBits::OrdinaryAllowedMask),
        DB::Exception);
}

class TrimMinMaxIndexWriteTest : public DB::base::TiFlashStorageTestBasic
{
protected:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        parent_path = getTemporaryPath();
        file_provider = db_context->getFileProvider();
    }

    static constexpr ColId settle_col_id = 100;

    ColumnDefinesPtr makeColumns()
    {
        auto cols = DMTestEnv::getDefaultColumns(DMTestEnv::PkType::HiddenTiDBRowID, /*add_nullable*/ false);
        cols->emplace_back(ColumnDefine{
            settle_col_id,
            "settle_time",
            std::make_shared<DataTypeMyDateTime>(0)});
        return cols;
    }

    Block makeBlockWithOutlier(size_t rows)
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, rows, /*reversed*/ false);
        auto type = std::make_shared<DataTypeMyDateTime>(0);
        auto col = type->createColumn();
        const UInt64 normal = MyDateTime(2020, 6, 1, 0, 0, 0, 0).toPackedUInt();
        const UInt64 sentinel = MyDateTime(2100, 1, 1, 0, 0, 0, 0).toPackedUInt();
        for (size_t i = 0; i < rows; ++i)
            col->insert(Field(i == 0 ? sentinel : normal));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "settle_time", settle_col_id));
        return block;
    }

    Block makeBlockInRangeOnly(size_t rows)
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, rows, /*reversed*/ false);
        auto type = std::make_shared<DataTypeMyDateTime>(0);
        auto col = type->createColumn();
        const UInt64 normal = MyDateTime(2020, 6, 1, 0, 0, 0, 0).toPackedUInt();
        for (size_t i = 0; i < rows; ++i)
            col->insert(Field(normal + i));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "settle_time", settle_col_id));
        return block;
    }

    DMFilePtr writeDMFile(const Block & block, bool enable_trim_write)
    {
        auto dm_file = DMFile::create(
            /*file_id*/ 1,
            parent_path,
            std::make_optional<DMChecksumConfig>(),
            128 * 1024,
            16 * 1024 * 1024,
            NullspaceID,
            DMFileFormat::V3);
        auto cols = makeColumns();
        DMFileWriter::Options options;
        options.enable_trim_minmax_write = enable_trim_write;
        DMFileWriter writer(dm_file, *cols, file_provider, db_context->getWriteLimiter(), options);
        writer.write(block, DMFileWriter::BlockProperty{0, 0, 0, 0});
        writer.finalize();
        return dm_file;
    }

    String parent_path;
    FileProviderPtr file_provider;
};

TEST_F(TrimMinMaxIndexWriteTest, WriteTrimIndexWhenOutliersExist)
try
{
    auto dm_file = writeDMFile(makeBlockWithOutlier(/*rows*/ 8), /*enable_trim_write*/ true);
    ASSERT_TRUE(dm_file->getColumnStat(settle_col_id).trim_minmax_index.has_value());
    EXPECT_EQ(dm_file->getColumnStat(settle_col_id).trim_minmax_index->pack_count(), dm_file->getPacks());
    EXPECT_EQ(dm_file->getColumnStat(settle_col_id).trim_minmax_index->format_version(), TrimMinMax::FormatVersionV1);

    const auto * meta = typeid_cast<const DMFileMetaV2 *>(dm_file->meta.get());
    ASSERT_NE(meta, nullptr);
    const auto fname = colTrimIndexFileName(DB::toString(settle_col_id));
    auto itr = meta->merged_sub_file_infos.find(fname);
    ASSERT_NE(itr, meta->merged_sub_file_infos.end());
    EXPECT_GT(itr->second.size, 0u);

    TrimMinMaxIndexMeta selected;
    auto reason = TrimMinMax::trySelectTrimMeta(
        /*read_enabled*/ true,
        dm_file->getColumnStat(settle_col_id).trim_minmax_index,
        *dm_file->getColumnStat(settle_col_id).type,
        dm_file->getPacks(),
        meta->merged_sub_file_infos,
        fname,
        &selected);
    EXPECT_EQ(reason, TrimMinMaxFallbackReason::None);
    EXPECT_EQ(selected.file_size, itr->second.size);

    // Restore and verify meta survives MetaV2 round-trip.
    auto restored = DMFile::restore(
        file_provider,
        1,
        1,
        parent_path,
        DMFileMeta::ReadMode::all(),
        /*meta_version*/ 0);
    ASSERT_TRUE(restored->getColumnStat(settle_col_id).trim_minmax_index.has_value());
}
CATCH

TEST_F(TrimMinMaxIndexWriteTest, SkipPersistWhenNoOutliers)
try
{
    auto dm_file = writeDMFile(makeBlockInRangeOnly(/*rows*/ 8), /*enable_trim_write*/ true);
    EXPECT_FALSE(dm_file->getColumnStat(settle_col_id).trim_minmax_index.has_value());

    const auto * meta = typeid_cast<const DMFileMetaV2 *>(dm_file->meta.get());
    ASSERT_NE(meta, nullptr);
    const auto fname = colTrimIndexFileName(DB::toString(settle_col_id));
    EXPECT_EQ(meta->merged_sub_file_infos.find(fname), meta->merged_sub_file_infos.end());
}
CATCH

TEST_F(TrimMinMaxIndexWriteTest, DisabledWriteSkipsTrim)
try
{
    auto dm_file = writeDMFile(makeBlockWithOutlier(/*rows*/ 8), /*enable_trim_write*/ false);
    EXPECT_FALSE(dm_file->getColumnStat(settle_col_id).trim_minmax_index.has_value());

    const auto * meta = typeid_cast<const DMFileMetaV2 *>(dm_file->meta.get());
    ASSERT_NE(meta, nullptr);
    const auto fname = colTrimIndexFileName(DB::toString(settle_col_id));
    EXPECT_EQ(meta->merged_sub_file_infos.find(fname), meta->merged_sub_file_infos.end());
}
CATCH

} // namespace DB::DM::tests
