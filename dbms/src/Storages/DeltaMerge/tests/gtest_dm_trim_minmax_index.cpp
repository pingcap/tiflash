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
#include <Interpreters/Settings.h>
#include <Storages/DeltaMerge/File/ColumnStat.h>
#include <Storages/DeltaMerge/File/DMFileUtil.h>
#include <Storages/DeltaMerge/File/MergedFile.h>
#include <Storages/DeltaMerge/Index/MinMaxIndex.h>
#include <Storages/DeltaMerge/Index/TrimMinMaxIndex.h>
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

} // namespace DB::DM::tests
